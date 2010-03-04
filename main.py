#!/usr/bin/env python

from google.appengine.ext import webapp, db
from google.appengine.ext.webapp import util
from google.appengine.api import urlfetch
from google.appengine.api.labs import taskqueue
import re
import gzip
import StringIO
import logging
import urllib
import yaml

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
      self.response.out.write("Not Found")

class FetchPackagesHandler(webapp.RequestHandler):
  def get(self):
    logging.info("Begin downloading 02packages.details.txt.gz")
    packages = urlfetch.fetch("http://cpan.cpantesters.org/modules/02packages.details.txt.gz")
    if packages.status_code != 200:
      logging.error('Download 02packages.details.txt.gz FAIL')
      self.response.out.write("FAIL")
      return

    logging.info('Download 02packages.details.txt.gz succeed.')

    result = urlfetch.fetch("http://cpan.cpantesters.org/authors/RECENT-6h.yaml")
    if result.status_code != 200:
      logging.error('Download RECENT-6h.yaml FAIL')
      self.response.out.write(result.content)
      return
    
    recent = yaml.load(result.content)
    is_recent = {}
    for update in recent['recent']:
      path = re.sub(r'^id/', r'', update['path'])
      is_recent[path] = True
    
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
        if data[2] in is_recent:
          pkgs.append(line)
          found = found + 1
          if found % 80 == 0:
            taskqueue.add(
              url='/work/update_packages',
              payload="\n".join(pkgs)
            )
            logging.info("Updated %d packages " % found)
            pkgs = []
    logging.info("Finishing...")
    self.response.out.write("Download success: %d" % found)

class UpdatedPackagesHandler(webapp.RequestHandler):
  def post(self):
    pkgs = []
    for line in self.request.body.split('\n'):
      data = re.split('\s+', line)
      pkg = Package(name=data[0], version=data[1], distribution=data[2])
      pkgs.append(pkg)
    db.put(pkgs)
    logging.info("Updated %d packages (from %s to %s)" % (len(pkgs), pkgs[0].name, pkgs[-1].name))
    self.response.out.write("Success")
    

def main():
  application = webapp.WSGIApplication([(r'/package/(.*)', PackageHandler),
                                        ('/work/fetch_packages', FetchPackagesHandler),
                                        ('/work/update_packages', UpdatedPackagesHandler),
                                        (r'/', MainHandler)
                                        ],
                                       debug=True)
  util.run_wsgi_app(application)


if __name__ == '__main__':
  main()
