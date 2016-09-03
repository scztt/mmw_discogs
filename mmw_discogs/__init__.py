
import urllib2
import time
import os.path
import re
import json

import discogs_client
import discogs_client.exceptions
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
from atomicfile import AtomicFile
import appdirs

SKIP_ARTISTS				= ['Peggy Lee', 'Kate Bush'] # sorry kate, you released too many records, they take too long to look up
ARTIST_NAME_MATCH_RATIO 	= 80
SAVE_DELAY					= 20
QUERY_DELAY 				= 0.05
CACHE_PATH 					= os.path.join(os.getcwd(), 'mmw_cache') #os.path.join(os.path.split(__file__)[0], 'cache')
MMW_MASTER_LIST_URL			= 'https://manymanywomen.com/master-index/'
DISCOGS_CLIENT_NAME			= 'MMW-Scanner/0.1'
DISCOGS_USER_TOKEN			= 'dEAyhpyGsLSfJAUadbuKisZmfNKghFwNGYPAOevZ'

discogs = discogs_client.Client(DISCOGS_CLIENT_NAME, user_token=DISCOGS_USER_TOKEN)
local_cache = {}
local_cache_last_saved = {}
label_id_cache = {}

try: os.mkdir(CACHE_PATH)
except Exception: pass

def is_an_artist_entry(entry):
	return not(entry.attrs.get('rel'))

# take an html node (entry) from th mmw master list and return a dict with artist names/url/etc
def parse_artist_info(entry):
	names = re.split(r'[,/]', entry.text)
	names = map(lambda name: name.strip(), names)
	names = map(lambda name: name.replace('/', ''), names)
	names = filter(lambda name: len(name) > 0, names)

	artist_info = {
		'names': 			names,
		'namestring':		entry.text.strip().strip('/'),
		'url': 				entry.attrs['href'],
		'discogs':  []
	}
	return artist_info

# load data from a cache file if it exists
def load_cache(name):
	if local_cache.get(name):
		return local_cache[name]
	else:
		cache_path = os.path.join(CACHE_PATH, name + '_cache.json')
		if os.path.exists(cache_path):
			with open(cache_path) as f:
				cache_str = f.read()
				cache_data = json.loads(cache_str)
				local_cache[name] = cache_data
				return cache_data
		return None

# save data to a cache file
def save_cache(name, data):
	local_cache[name] = data
	last_saved = local_cache_last_saved.get(name, -1)
	now = time.time()
	if now - last_saved > SAVE_DELAY:
		local_cache_last_saved[name] = now
		cache_path = os.path.join(CACHE_PATH, name + '_cache.json')
		cache_str = json.dumps(data, indent=4)
		with AtomicFile(cache_path, 'w') as f:
			f.write(cache_str)
			f.close()

# a wrapper around a function that provides caching and retry functionality
# @cached('name') will cache the output of the function in a filed named 'name_cache.json'
# @cached('name', 2) will use a dict structure for the cache, with function argument #2 used as the key value for the dict
# @cached('name', always_write=True) will always write the value to the cache - use this for setter functions (e.g. places were youre always setting a value, rather than retrieving a previously set value)
def cached(name, key=None, always_write=False):
	def wrapped_func(func):
		def cache_and_retry(*args):
			if (isinstance(key, int)):
				cache_key = args[key]
			elif key != None:
				cache_key = key
			else:
				cache_key = None

			cached = load_cache(name)

			if cache_key != None:
				cache_item = None
				if cached != None:
					cache_item = cached.get(cache_key)
				else:
					cached = {}

				if cache_item != None and not(always_write):
					result = cache_item
				else:
					cached[cache_key] = result = retry_func(func, args)
					save_cache(name, cached)
			else:
				if cached != None and not(always_write):
					result = cached
				else:
					cached = result = retry_func(func, args)
					save_cache(name, cached)

			return result
		return cache_and_retry
	return wrapped_func

# calls func(*args) max_tries times, retrying with an increasing pause if there is an HTTP error
def retry_func(func, args, max_tries=30):
	trys = 0
	last_exception = None

	while trys < max_tries:
		try:
			return func(*args)
		except discogs_client.exceptions.HTTPError, e:
			if e.status_code == 404:
				return None
			else:
				last_exception = e
				trys += 1
				time.sleep((trys ** 1.5) * QUERY_DELAY)

	raise last_exception

# return a dict containing the artist entries from the mmw master index
@cached('get_artist_entries')
def get_artist_entries():
	mmw_index_html = BeautifulSoup(urllib2.urlopen(MMW_MASTER_LIST_URL), 'html.parser')
	entries = mmw_index_html.select('div.entry-content a[target="_blank"]')
	entries = filter(is_an_artist_entry, entries)
	artists = map(parse_artist_info, entries)

	results = {}
	for artist in artists:
		name = artist['namestring'].strip()
		if results.get(name):
			results[name]['url'] = artist['url']
			results[name]['names'] = artist['names']
		else:
			results[name] = artist
	return results

# for an artist name, return all good matches (based on a fuzzy text comparison) from discogs
@cached('get_artist_discogs', 0)
def get_artist_discogs(artist_name):
	limit = 10

	print 'Discogs search for %s...' % artist_name

	artist_discogs_entries = []
	results = discogs.search(artist_name, type='artist')

	for result in results:
		limit -= 1
		if limit == 0: break

		found_name = result.data['name']
		match_ratio = fuzz.ratio(artist_name.lower(), found_name.lower())
		#print '\t%s: %s' % (found_name, match_ratio)

		if match_ratio >= ARTIST_NAME_MATCH_RATIO:
			print '    -> %s' % found_name
			# Hold on to valid discogs entries for later use
			artist_discogs_entries.append({
				'name':		 	result.name,
				'artist_id': 	result.id,
			})

	return artist_discogs_entries

# collect all matching discogs entires for a list of artist names
def get_artists_discogs(artist_names):
	discogs_entries = []
	for artist_name in artist_names:
		artist_discogs = get_artist_discogs(artist_name)
		discogs_entries = discogs_entries + artist_discogs
	return discogs_entries

roles = set()

def get_release_info(release):

	main_release = release
	if 'main_release' in dir(release): main_release = release.main_release
	is_compilation = False
	artist_count  = float(len(main_release.artists))
	track_count = float(len(main_release.tracklist))

	if (len(main_release.tracklist) > 4 and (artist_count / track_count) > 0.75)\
			or filter(lambda a: a.name == 'Various', main_release.artists):
		is_compilation = True

	release_info = {
		'id': 			release.id,
		'title':		release.title,
		'compilation':	is_compilation,
		'artists':		map(lambda a: a.name, main_release.artists),
		'labels':		[],
	}

	for label in main_release.labels:
		label_id_cache[label.name] = label.id
		release_info['labels'].append({
			'id': label.id,
			'name': label.name
		})

	return release_info

# for an artist name and discogs id for that artist, collect all releases for that artist
@cached('get_releases_for_artist', 0)
def get_releases_for_artist(artist, id):
	releases = []
	for release in discogs.artist(id).releases:
		release_info = retry_func(get_release_info, [release])
		if release_info:
			releases.append(release_info)
	return releases

@cached('get_release_count_for_label', 0)
def get_release_count_for_label(label_name, id):
	if not(id):
		id = label_id_cache.get(label_name)
	if id:
		label = discogs.label(id)
		return len(label.releases)
	else:
		return 0

# get collected info about a label
@cached('label_info', 0)
def get_label_info(label):
	return {
		'name': 	label,
		'id':		label_id_cache.get(label, 0),
		'artists': 	{},
	}

# store collected info about a label
@cached('label_info', 0, always_write=True)
def set_label_info(label, info):
	return info

# get a list of all label info
@cached('label_info')
def get_all_labels():
	return {}

# add an artist's release to the info for a label
def add_release_to_label(label, artist, release):
	label_info = get_label_info(label)
	if not(label_info['artists'].get(artist)):
		label_info['artists'][artist] = []
	if not(release in label_info['artists'][artist]):
		label_info['artists'][artist].append(release)
		set_label_info(label, label_info)


########################################################################################################
def run():
	artist_entries = get_artist_entries()

	for name in sorted(artist_entries.keys()):
		if not(name in SKIP_ARTISTS):
			artist = artist_entries[name]
			artist_name = artist['namestring']

			artist_discogs = get_artists_discogs(artist['names'])

			releases_dict = {}
			for discogs_entry in artist_discogs:
				releases = get_releases_for_artist(artist_name, discogs_entry['artist_id']) or []
				for release in releases:
					releases_dict[release['title']] = release

			def release_is_by_artist(release, artist_names):
				if release['compilation']:
					print '\t\t- %s (skipping compilation)' % release['title']
					return False
				artist_names = set(map(lambda a: a.lower(), artist_names))
				release_names = set(map(lambda a: a.lower(), release['artists']))
				if not(artist_names.intersection(release_names)):
					print '\t\t- %s (release not by artist, %s)' % (release['title'], ', '.join(release['artists']))
					return False
				else:
					print '\t\t+ %s' % release['title']
					return True

			names = artist['names'] + map(lambda d: d['name'], artist_discogs)
			filtered_releases = filter(lambda r: release_is_by_artist(r, names), releases_dict.values())
			for release in filtered_releases:
				for label in release['labels']:
					add_release_to_label(label['name'], artist_name, release['title'])

	all_labels = get_all_labels()
	label_stats = []
	for label_info in all_labels.values():
		artists = label_info['artists']
		releases = set()
		for artist in artists:
			releases.update(label_info['artists'][artist])

		label_stats.append({
			'label': 			label_info,
			'artists': 			len(artists),
			'releases':			len(releases),
		})

	by_artists = sorted(label_stats, key=lambda l: l['artists'])
	by_artists.reverse()
	by_releases = sorted(label_stats, key=lambda l: l['releases'])
	by_releases.reverse()

	print '\n\n\n'
	print 'TOP LABELS BY RELEASE COUNT'
	for label in by_releases[0:50]:
		if not(label['label'].get('id')): label['label']['id'] = discogs.search(label['label']['name'], type='label')[0].id
		release_count = get_release_count_for_label(label['label']['name'], label['label'].get('id'))
		release_pct = 0
		if release_count:
			release_pct = int(label['releases'] * 100.0 / release_count)

		print '\t[ %s ]  %s releases (%d%%, %d total releases)' % (
			label['label']['name'].rjust(25),
			str(label['releases']).rjust(4),
			release_pct,
			release_count
		)

	print 'TOP LABELS BY ARTIST COUNT'
	for label in by_artists[0:50]:
		if not(label['label'].get('id')): label['label']['id'] = discogs.search(label['label']['name'], type='label')[0].id
		release_count = get_release_count_for_label(label['label']['name'], label['label'].get('id'))

		print '\t[ %s ]  %s artists (%d total releases)' % (
			label['label']['name'].rjust(25),
			str(label['artists']).rjust(4),
			release_count
		)
