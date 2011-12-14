#!/usr/bin/env python

from google.appengine.ext import webapp, db
from google.appengine.ext.webapp import util
from google.appengine.api import urlfetch, users, memcache
from google.appengine.api.labs import taskqueue
import re, gzip, StringIO, logging, urllib, yaml
import simplejson as json

def work_queue_only(func):
  """Decorator that only allows a request if from cron job, task, or an admin.

  Args:
    func: A webapp.RequestHandler method.

  Returns:
    Function that will return a 401 error if not from an authorized source.
  """
  def decorated(myself, *args, **kwargs):
    if ('X-AppEngine-Cron' in myself.request.headers or
        'X-AppEngine-TaskName' in myself.request.headers or
        users.is_current_user_admin()):
      return func(myself, *args, **kwargs)
    elif users.get_current_user() is None:
      myself.redirect(users.create_login_url(myself.request.url))
    else:
      myself.response.set_status(401)
      myself.response.out.write('Handler only accessible for work queues')
  return decorated

class Package(db.Model):
  name = db.StringProperty()
  version = db.StringProperty(indexed=False)
  distribution = db.StringProperty()

class MainHandler(webapp.RequestHandler):
  def get(self):
    self.response.set_status(302)
    self.response.headers['Location'] = 'http://cpanmetadb.plackperl.org/'
    self.response.out.write('Redirecting')
    #self.response.out.write(open('./index.html').read())
  
class PackageHandler(webapp.RequestHandler):
  def get(self, version, package):
#    return self.get_metacpan(version, package)
#    return self.get_db(version, package)
    self.response.set_status(302)
    self.response.headers['Location'] = 'http://cpanmetadb.plackperl.org/v1.0/package/' + urllib.unquote(package)
    self.response.out.write('Redirecting')

  def get_json(self, url):
    res = urlfetch.fetch(url)
    return json.loads(res.content)

  def memcached(func):
    def decorated(*args):
      key = ':'.join((func.__name__, args[1]))
      cached = memcache.get(key)
      if cached is not None:
        logging.debug('Cache hit: %s' % args[1])
        return cached
      logging.debug('Cache MISS: %s' % args[1])
      val = func(*args)
      memcache.set(key, val, time=3600)
      return val
    return decorated

  @memcached
  def fetch_metacpan(self, package):
    try:
      meta = self.get_json('http://api.metacpan.org/module/%s' % package)
      if 'distribution' in meta:
        dist = self.get_json('http://api.metacpan.org/release/%s' % meta['distribution'])
        distfile = re.sub('.*/authors/id/', '', dist['download_url'])
        version = 'undef'
        for module in meta['module']:
          if str(module['name']) == package:
            version = module.get('version', 'undef')
        return { 'distfile': distfile, 'version': version }
    except Exception, e:
      logging.exception(e)
    return 0  # for memcache

  def get_metacpan(self, version, package):
    package = urllib.unquote(package)
    try:
      module = self.fetch_metacpan(package)
      if module:
        self.response.headers['Content-Type'] = 'text/x-yaml'
        self.response.out.write("---\ndistfile: %s\nversion: %s\n" % (module['distfile'], module['version']))
        return
    except Exception, e:
      logging.exception(e)
    self.response.set_status(404)

  def get_db(self, version, package):
    query = Package.all()
    query.filter('name = ', urllib.unquote(package))
    package = query.get()
    if package != None:
      self.response.headers['Content-Type'] = 'text/x-yaml'
      self.response.out.write("---\ndistfile: %s\nversion: %s\n" % (package.distribution, package.version))
      return
    self.response.set_status(404)

class FetchPackagesHandler(webapp.RequestHandler):
  hosts = [
#    'http://cpan.cpantesters.org',
    'http://cpan.metacpan.org',
    'http://cpan.hexten.net',
    'http://cpan.dagolden.com',
  ]

  @work_queue_only
  def get(self, bootstrap):
    for host in self.hosts:
      logging.info("Begin downloading 02packages.details.txt.gz from %s" % host)

      status = None
      try:
        packages = urlfetch.fetch("%s/modules/02packages.details.txt.gz" % host)
        status = packages.status_code
      except:
        status = 500

      if status != 200:
        logging.error('Download 02packages.details.txt.gz from %s FAIL' % host)
        self.response.out.write("FAIL")
        continue

      logging.info('Download 02packages.details.txt.gz from %s succeed. Last-Modified: %s' % (host, packages.headers['Last-Modified']))

      is_recent = []
      if (not bootstrap):
        result = urlfetch.fetch("%s/authors/RECENT-1d.yaml" % host)
        if result.status_code != 200:
          logging.error('Download RECENT-1d.yaml FAIL')
          self.response.out.write(result.content)
          continue
    
        recent = yaml.load(result.content)
        for update in recent['recent']:
          path = re.sub(r'^id/', r'', update['path'])
          is_recent.append(path)

      self.update_packages(packages, is_recent, bootstrap)
      break

  def update_packages(self, packages, is_recent, bootstrap):
    file = gzip.GzipFile(fileobj = StringIO.StringIO(packages.content))

    header_is_done = False
    found = 0
    pkgs = []

    for line in file:
      line = line.rstrip()
      if line == '':
        header_is_done = True
      elif header_is_done:
        data = re.split('\s+', line)
        if len(data) == 3 and (bootstrap or (data[2] in is_recent)):
          pkgs.append(line)
          found = found + 1
          if found % 50 == 0:
            taskqueue.add(
              url='/work/update_packages',
              payload="\n".join(pkgs)
            )
            logging.info("Updated %d packages " % found)
            pkgs = []

    taskqueue.add(
      url='/work/update_packages',
      payload="\n".join(pkgs)
    )

    logging.info("Found %d packages. Finishing..." % found)
    self.response.out.write("Download success: %d" % found)

class UpdatedPackagesHandler(webapp.RequestHandler):

  @work_queue_only
  def post(self):
    pkgs = []
    for line in self.request.body.split('\n'):
      data = re.split('\s+', line)
      pkg = Package(name=data[0], version=data[1], distribution=data[2])
      pkgs.append(pkg)

    new_keys = []
    db.put(pkgs)
    for p in pkgs:
      new_keys.append(p.key())

    for p in pkgs:
      query = Package.all()
      query.filter('name = ', p.name)
      pkgs_in_db = query.fetch(10)
      for pkg_in_db in pkgs_in_db:
        if not pkg_in_db.key() in new_keys:
          logging.debug("Deleting stale %s" % pkg_in_db.name)
          pkg_in_db.delete()

    logging.info("Updated %d packages (from %s to %s)" % (len(pkgs), pkgs[0].name, pkgs[-1].name))
    self.response.out.write("Success")

def main():
  application = webapp.WSGIApplication([(r'/v([0-9\.]+)/package/(.*)', PackageHandler),
                                        ('/work/fetch_packages/?(.*)', FetchPackagesHandler),
                                        ('/work/update_packages', UpdatedPackagesHandler),
                                        (r'/', MainHandler)
                                        ],
                                       debug=True)
  util.run_wsgi_app(application)


if __name__ == '__main__':
  main()
