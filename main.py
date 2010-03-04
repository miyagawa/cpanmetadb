#!/usr/bin/env python

from google.appengine.ext import webapp, db
from google.appengine.ext.webapp import util
from google.appengine.api import urlfetch
from google.appengine.api.labs import taskqueue
from google.appengine.api import users
import re
import gzip
import StringIO
import logging
import urllib
import yaml

def work_queue_only(func):
  """Decorator that only allows a request if from cron job, task, or an admin.

  Also allows access if running in development server environment.

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
    self.response.out.write(open('./index.html').read())
  
class PackageHandler(webapp.RequestHandler):
  def get(self, package):
    query = Package.all()
    query.filter('name = ', urllib.unquote(package))
    package = query.get()
    if package != None:
      self.response.headers['Content-Type'] = 'text/x-yaml'
      self.response.out.write("---\ndist: %s\nversion: %s\n" % (package.distribution, package.version))
    else:
      self.response.set_status(404)

class FetchPackagesHandler(webapp.RequestHandler):
  @work_queue_only
  def get(self, bootstrap):
    logging.info("Begin downloading 02packages.details.txt.gz")
    packages = urlfetch.fetch("http://cpan.cpantesters.org/modules/02packages.details.txt.gz")
    if packages.status_code != 200:
      logging.error('Download 02packages.details.txt.gz FAIL')
      self.response.out.write("FAIL")
      return

    logging.info('Download 02packages.details.txt.gz succeed. Last-Modified: %s' % packages.headers['Last-Modified'])

    is_recent = []
    if (not bootstrap):
      result = urlfetch.fetch("http://cpan.cpantesters.org/authors/RECENT-6h.yaml")
      if result.status_code != 200:
        logging.error('Download RECENT-6h.yaml FAIL')
        self.response.out.write(result.content)
        return
    
      recent = yaml.load(result.content)
      for update in recent['recent']:
        path = re.sub(r'^id/', r'', update['path'])
        is_recent.append(path)
    
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
        if bootstrap or (data[2] in is_recent):
          pkgs.append(line)
          found = found + 1
          if found % 50 == 0:
            taskqueue.add(
              url='/work/update_packages',
              payload="\n".join(pkgs)
            )
            logging.info("Updated %d packages " % found)
            pkgs = []
    logging.info("Finishing...")
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
  application = webapp.WSGIApplication([(r'/package/(.*)', PackageHandler),
                                        ('/work/fetch_packages/?(.*)', FetchPackagesHandler),
                                        ('/work/update_packages', UpdatedPackagesHandler),
                                        (r'/', MainHandler)
                                        ],
                                       debug=True)
  util.run_wsgi_app(application)


if __name__ == '__main__':
  main()
