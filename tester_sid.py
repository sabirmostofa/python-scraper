from bs4 import BeautifulSoup
import opener, configs, re, sys, socket, random, math,time, multiprocessing, threading
from urlparse import urlparse
from base64 import standard_b64decode
import MySQLdb as mdb
from datetime import datetime
from multiprocessing import Pool



socket.setdefaulttimeout(15)
WORKER = 7
THREAD_NUM = 4
TEST = 0

def get_series_id_in_database(name,link,d,con):
	
	cur=con.cursor(mdb.cursors.DictCursor)
	cur.execute("SELECT * FROM `vs_series` WHERE `series_name`=%s",name)
	res=cur.fetchone()
	if res:
		return res['series_id']
	else :
		sql='''INSERT ignore INTO `vs_series`(`series_name`, `imdb_link`, `series_release_date`) VALUES (%s,%s,%s)'''
		args=(name,link,d)
		cur.execute(sql,args)
		return get_series_id_in_database(name,link,d,con)

def set_season_episode(series_id,episode_link,season,episode,con):
	
	cur=con.cursor()
	cur.execute('SELECT * FROM `vs_series_links` WHERE `series_id`=%s AND `link_url`=%s',(series_id,episode_link))
	res=cur.fetchone()
	if res:
		return
	else :
		sql='''INSERT ignore INTO `vs_series_links`(`series_id`, `season`, `episode`, `link_url`) VALUES (%s,%s,%s,%s)'''
		args=(series_id,season,episode,episode_link,)
		cur.execute(sql,args)

def i_have_got_series_episode_url(name,series_id,url,season,episode,con):
	#print '\n\nName: %s\nSeason: %s .Episode: %s' %(name,season,episode)
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data)
	l=soup.find_all('a')
	reg=re.compile(r'.*?url=(.+?)&domain.*')
	reg2=re.compile(r'.*external.php.*')
	
	for i in l:
		if not i.has_key('href'):
			continue
		ref=i['href']
		parsed=urlparse(ref)
		try:
			t1=parsed[2]
			if not reg2.match(t1):
				continue
			m=reg.match(parsed[4])
			final_url=standard_b64decode(m.group(1))
			set_season_episode(series_id,final_url,season,episode,con)
		except:
			pass
	



def i_have_got_series_name((url,name,con)):
	#~ print 'Series:%s#%s' %(name,url)
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data)
	
	released_date=datetime.today()
	
	try:
		l=soup.select('.movie_info > table ')
		l=(l[0].find_all('tr'))
		l=l[1].find_all('td')[1].text
		released_date=datetime.strptime(l,'%B %d, %Y')
	except:
		pass
	
	imdb_link="-1"
	try:
		imdb_link=soup.select('.mlink_imdb')[0].find_all('a')
		imdb_link=imdb_link[0].get('href')
	except:
		pass
	
	series_id_in_database=get_series_id_in_database(name,imdb_link,released_date,con)
	
	l=soup.find_all('a')
	t1=url;
	t1=t1.replace('http://www.1channel.ch/watch','tv')
	t1='/'+t1+"/season-(\d+)-episode-(\d+).*"
	reg=re.compile(t1)
	
	for i in l:
		if not i.has_key('href'):
			continue
		m=reg.match(i.get('href'))
		if m:
			episode_link="http://www.1channel.ch"+m.group(0)
			season=m.group(1)
			episode=m.group(2)
			i_have_got_series_episode_url(name,series_id_in_database,episode_link,season,episode,con)
	




def i_have_got_page_number(url):
	
	data=opener.fetch(url)['data']
	#~ url_to = '%s.html'%i
	#~ f=open(url_to,'w')
	#~ f.write(data)
	#~ return
	soup=BeautifulSoup(data)
	l=soup.find_all('a')
	reg=re.compile(r'.*/watch-\d+-(.*)')
	for i in l:
		if not i.has_key('href'):
			continue;
		if not i.has_key('title'):
			continue;
		link =i.get('href')
		m=reg.match(link)
		if m:
			series_name=i.get('title')
			if 'Watch' in series_name:
				series_name=series_name[6:-7].strip()
			series_link="http://www.1channel.ch"+m.group(0)
			i_have_got_series_name(series_link,series_name)


def get_page_count_and_go_deeper(url):
	#~ data=opener.fetch(url)['data']
	#~ soup=BeautifulSoup(data)
	#~ l=soup.select('.pagination > a ')
	#~ ref = l[len(l)-1]['href']
	#~ reg=re.compile(r'.*?page=(\d+).*?')
	#~ page_count=1
	#~ m=reg.match(ref)
	#~ if m:
		#~ page_count=int(m.group(1))
	#~ print page_count
	#~ 
	#~ 
	#~ for i in range(1,page_count+1):
		#~ new_url=url+"=&page="+str(i)
		#~ print new_url
		new_url=url
		i_have_got_page_number(new_url)
		

def get_all_series_links(pages):
	(all_series, counter)=([], 0)
	
	for url in pages:
		counter+=1
		if TEST:
			if counter==2:
				break
		data=opener.fetch(url)['data']
		#~ url_to = '%s.html'%i
		#~ f=open(url_to,'w')
		#~ f.write(data)
		#~ return
		soup=BeautifulSoup(data)
		l=soup.find_all('a')
		reg=re.compile(r'.*/watch-\d+-(.*)')
		for i in l:
			if not i.has_key('href'):
				continue;
			if not i.has_key('title'):
				continue;
			link =i.get('href')
			m=reg.match(link)
			if m:
				series_name=i.get('title')
				if 'Watch' in series_name:
					series_name=series_name[6:-7].strip()
				series_link="http://www.1channel.ch"+m.group(0)
				all_series.append((series_name, series_link))
	
	return all_series
	
	
def get_all_series_links_thread(tot):
	
	#print 'inside a thread@@@@@@@@@@@@@!!!!!!!!!!!!'
	counter=0
	for url in tot:
		#print 'getting from: %s' % url
		counter+=1
		if TEST:
			if counter==2:
				break
		data=opener.fetch(url)['data']
		#~ url_to = '%s.html'%i
		#~ f=open(url_to,'w')
		#~ f.write(data)
		#~ return
		soup=BeautifulSoup(data)
		l=soup.find_all('a')
		reg=re.compile(r'.*/watch-\d+-(.*)')
		for i in l:
			if not i.has_key('href'):
				continue;
			if not i.has_key('title'):
				continue;
			link =i.get('href')
			m=reg.match(link)
			if m:
				series_name=i.get('title')
				if 'Watch' in series_name:
					series_name=series_name[6:-7].strip()
				series_link="http://www.1channel.ch"+m.group(0)
				all_series.append((series_name, series_link))
	
	
	
	
	# starting multiprocess
def start_multiprocessing((series_name, series_link)):
	series_name=re.sub("&#(\d+)(;|(?=\s))", _callback, series_name)
	con=mdb.connect(configs.HOST,configs.USER,configs.PASS,configs.DB)
	#print 'Process started: %s' % multiprocessing.current_process()
	try:
		i_have_got_series_name((series_link,series_name,con))
	except:
		print 'error after mapping'
	#print 'Process ended: %s' % multiprocessing.current_process()
	con.close()
	
	

def generate_all_the_main_page_name():
	l=[]
	l.append('http://www.1channel.ch/?letter=123&tv')
	#~ if TEST:
		#~ return l
	for i in range(ord('a'),ord('z')+1):
		l.append('http://www.1channel.ch/?letter='+str(chr(i))+'&tv')
	
	#generating all pages with page number	
	all_pages = [] 
	for url in l:
		page_count=1
		data=opener.fetch(url)['data']
		soup=BeautifulSoup(data)
		l=soup.select('.pagination > a ')		
		
		if len(l) != 0:
			ref = l[len(l)-1]['href']
			reg=re.compile(r'.*?page=(\d+)')
			#~ print url, l[len(l)-1]['href']
			m=reg.match(ref)
			if m:
				page_count=int(m.group(1))
				#~ print page_count
		for i in range(1,page_count+1):
			all_pages.append(url+"=&page="+str(i))
	return all_pages

def generate_main_pages_thread(url):
	#print url
	page_count=1
	data=opener.fetch(url)['data']
	soup=BeautifulSoup(data)
	l=soup.select('.pagination > a ')		
		
	if len(l) != 0:
		ref = l[len(l)-1]['href']
		reg=re.compile(r'.*?page=(\d+)')
		#~ print url, l[len(l)-1]['href']
		m=reg.match(ref)
		if m:
			page_count=int(m.group(1))
			#~ print page_count
	for i in range(1,page_count+1):
		tot.append(url+"=&page="+str(i))
	
	
def chunks(l, n):
	return [l[i:i+n] for i in range(0, len(l), n)]

def _callback(matches):
    id = matches.group(1)
    try:
        return unichr(int(id))
    except:
        return id


if __name__=='__main__':
	
	#~ print generate_all_the_main_page_name()	
	#~ i_have_got_series_name("http://www.1channel.ch/watch-9460-2020","hello")
	#~ i_have_got_page_number('http://www.1channel.ch/?letter=123&tv&page=1')
	#~ get_page_count_and_go_deeper('http://www.1channel.ch/?letter=123&tv')
	
	
	#Generating all the page links using threading
	pages = []
	tot=[]
	pages.append('http://www.1channel.ch/?letter=123&tv')
	for i in range(ord('a'),ord('z')+1):
		pages.append('http://www.1channel.ch/?letter='+str(chr(i))+'&tv')
	
	
	thread_pages=[]
	for i in range(len(pages)):
		threadp = threading.Thread(target=generate_main_pages_thread, args=(pages[i],))
		threadp.start()
		thread_pages.append(threadp)
		
	for thread in thread_pages:
		thread.join()
		
	print 'initial Threading done PAGES FOUND %s ' % len(tot)
	#~ tot = generate_all_the_main_page_name()
	random.shuffle(tot)
	
	# Generating all the series links using threading
	all_series=[]
	if len(tot)>THREAD_NUM:
		thread_numbers=THREAD_NUM
	else:
		thread_numbers=1
		
	chunk_length = int(math.ceil(len(tot)/THREAD_NUM))
	if chunk_length == 0:
		chunk_length = 1
	thread_chunks=chunks(tot, chunk_length )
	threads=[]
	
	print 'Pages split into: %s' % len(thread_chunks)
	
	for i in range(thread_numbers):
		#~ get_all_series_links_thread(thread_chunks[i])
		#~ continue
		thread = threading.Thread(target=get_all_series_links_thread, args=(thread_chunks[i],))
		thread.start()
		threads.append(thread)
	for thread in threads:
		thread.join()
	import marshal
	#marshalling series
	f=open('marshalled','wb')
	marshal.dump(all_series, f)
	f.close()
	print 'done'
	sys.exit()
	print 'threading done'
	print 'TOTAL SERIES %s' % len(all_series)
	all_series=[(s.encode('ascii', 'xmlcharrefreplace'),i) for s,i in all_series ]
	error_counter=0
	for  i in all_series:
		try:
			i[1].encode('ascii')
		except:
			error_counter+=1
			print i[1]
	print error_counter
	sys.exit()
	#~ all_series=get_all_series_links(tot)		
	random.shuffle(all_series)
	#default encoding ASCII error proned
	#all_series=[(s.encode('latin-1', 'xmlcharrefreplace'),i) for s,i in all_series ]
	print all_series[0]
	#sys.exit()
	#~ sys.exit()
	print 'Total pages to loop over: %s' % len(tot)
	print 'Total Series to loop over: %s' % len(all_series)
	#~ print tot
	#~ print len(tot)
	#~ sys.exit()
	
	# Starting multiprocessing
	p=Pool(processes=WORKER)
	chunk_size = int(math.ceil(len(all_series)/WORKER))
	print 'Chunk Size set to: %s' % chunk_size	
	
	try:
		p.map(start_multiprocessing , all_series)
	except:
		print 'Process Allocation Error^^^^^^^^^^^^^^^^^'
	
		
	#~ p.map_async(get_page_count_and_go_deeper,tot)
	
