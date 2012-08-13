import urllib2

proxy  = urllib2.ProxyHandler({'http': '187.120.209.43:8080'})
opener = urllib2.build_opener(proxy)
urllib2.install_opener(opener)
my_ip = urllib2.urlopen('http://whatthehellismyip.com/?ipraw').read()

print my_ip
