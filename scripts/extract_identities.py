""" 
Extract keywords from COVID Twitter data

Saves out to a JSON file per each input tweet JSON dump
"""


import re
import pdb
from multiprocessing import Pool
from collections import Counter
import os
from glob import glob
import gzip
import json
import shutil

from tqdm import tqdm
import pandas as pd


def remove_mentions(text, user_mentions):
    """ Remove mentions from a text """
    new_text = text
    usernames = [mention['screen_name'] for mention in user_mentions]
    for username in usernames:
        new_text = re.sub(username, 'USER', new_text, flags=re.IGNORECASE)
    return new_text


def process_text(text, user_mentions):
    new_text = remove_mentions(text, user_mentions)
    return ' '.join(new_text.split(' ')[:-1]) # remove URL


def process_tweets(data):
    """ Process a dataframe of tweets """
    selected_cols = ['created_at', 'id_str', 'text', 'in_reply_to_status_id_str', 'geo', 'coordinates', 'place', 
                 'is_quote_status', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'possibly_sensitive',
                 'search_match',
                 'user.id_str', 'user.location', 'user.geo_enabled',
                 'entities.hashtags', 'entities.urls', 'entities.user_mentions', 'entities.symbols', 'entities.media', 'extended_entities.media',
                 'retweeted_status.id_str', 'retweeted_status.user.location', 'retweeted_status.geo', 'retweeted_status.coordinates', 'retweeted_status.place', 
                 'retweeted_status.quote_count', 'retweeted_status.reply_count', 'retweeted_status.retweet_count',
                 'quoted_status.id_str', 'quoted_status.user.location', 'quoted_status.geo', 'quoted_status.coordinates', 'quoted_status.place', 
                 'quoted_status.quote_count', 'quoted_status.reply_count', 'quoted_status.retweet_count',
                ]
    # Check if columns are present
    selected_cols = [col for col in selected_cols if col in data.columns]
    data = data[selected_cols].copy()
    data['url'] = [el[-1] for el in data.text.str.split(' ')]
    data['processed_text'] = [process_text(text, mentions) for text, mentions in zip(data['text'], data['entities.user_mentions'])]
    data = data[sorted(data.columns)]
    return data.drop(columns='text')

def match_identities(text, identity_pat):
    """ Search within a text for identity matches, return them and the spans they occur in """
    #all_matches = re.findall(identity_pat, str(text).lower())
    all_matches = list(re.finditer(identity_pat, str(text)))
    #limit = 20 # limit for number of unique identity mentions for each post
    
    res = []
    spans = []
    #ctr = Counter()
    for match in all_matches:
        match_text = match.group()
        match_span = match.span()
        #ctr[match_text] += 1
        #if ctr[match_text] > limit:
        #    continue
        #else:
        res.append(match_text)
        spans.append(match_span)

    # Counter method (is slightly slower)
    #ctr = Counter(all_matches)
    #res = sum([[el] * min(count, limit) for el, count in ctr.items()], [])
    return res, spans


class IdentityExtractor():

    def __init__(self, load_vocab=False, overwrite=False):
        """ Args:
            load_vocab: If False, will build unigram vocab from all the Twitter dumps
            overwrite: If True, will overwrite any existing files already saved out from extraction
        """
        self.load_vocab = load_vocab
        self.overwrite = overwrite
        self.identities_path = '../resources/antisemitic_terms.txt'
        self.identities = None
        self.basepath = '/storage3/coronavirus/'
        self.paths = None
        self.n_selected = 0

    def load_identities(self, vocab):
        """ Load, filter identities to just those that are present in the vocab
            Args:
                vocab: a set of unique strings
        """
        print("Loading, filtering identity list...")
        identities = pd.read_excel(self.identities_path).drop_duplicates('English')['English'].tolist()
        self.identities = [term for term in identities if term in vocab]
        print(f"\t{len(self.identities)} present in vocabulary out of {len(identities)}")
        self.pats = [re.compile(r'\b{}\b'.format(re.escape(term))) for term in self.identities]

    def select_tweet(self, tweet):
        """ See if a tweet is worth keeping (matches enough criteria).
            Extract identities from user bio if tweet does match criteria.
        """
        select = False
        
        # Basic cleaning
        if len(tweet) == 1 and 'limit' in tweet:
            return select
        
        # Language is English
        # TODO: change to checking for user language
        #if tweet['lang'] != 'en':
        #    return select

        # Check for containing a user description
        if not 'user' in tweet:
            return select
        if tweet['user']['description'] is None:
            return select
        
        # Contains identity terms
        #if 'extended_tweet' in tweet:
        #    text = tweet['extended_tweet']['full_text'].lower()
        #else:
        #    text = tweet['text'].lower()
        matches, spans = match_identities(tweet['user']['description'])
        #for p in self.pats:
        #    m = re.search(p, text)
        #    # if any([re.search(p, tweet['extended_tweet']['full_text'].lower()) for p in pats]):
        #    if m is not None:
        #        select = m.group()
        #        # tqdm.write('one selected')
                
        return select

    def load_process_tweets(self):
        """ Load tweets that have been filtered and saved already.
            Use if you want to do additional processing
        """
        filtered_dirpath = os.path.join('../output', 'tweets_json')
        out_dirpath = os.path.join('../output', 'processed_tweets_csv')
        if not os.path.exists(out_dirpath):
            os.mkdir(out_dirpath)
        for fname in tqdm(sorted(os.listdir(filtered_dirpath)), ncols=80):
            fpath = os.path.join(filtered_dirpath, fname)
            with open(fpath) as f:
                data = pd.json_normalize([json.loads(line) for line in f.read().splitlines()])
            if not len(data) == 0:
                processed = process_tweets(data)
        
            # Save out
            outpath = os.path.join(out_dirpath, f'{os.path.splitext(fname)[0]}.csv')
            processed.to_csv(outpath)

    def process_dump(self, fpath):
        """ Process a jsonlines dumped Twitter file """
        selected = []
        fname = os.path.basename(fpath)
        outpath = os.path.join('../output', 'tweets_json', f'{fname.split(".")[0]}.jsonl')
        #csv_outpath = os.path.join('../output', 'tweets_csv', f'{fname.split(".")[0]}.csv')
        if os.path.exists(outpath) and not self.overwrite: # already processed
            return

        #tqdm.write(fname)
        with gzip.open(fpath, 'rb') as f:
            #for i, line in tqdm(enumerate(f), total=974483):
            # for i, line in tqdm(enumerate(f), total=974483, bar_format='selected: {postfix} | Elapsed: {elapsed} | {rate_fmt}', postfix=n_selected):
            for line in f:
                if len(line) == 1:
                    continue
                try:
                    tweet = json.loads(line)
                except json.decoder.JSONDecodeError:
                    tqdm.write('json decode error')
                    continue
                except UnicodeDecodeError:
                    tqdm.write('unicode decode error')
                    continue
                match = self.select_tweet(tweet)
                if match:
                    tweet['search_match'] = match
                    selected.append(tweet)
                # if i > 100:

        # Save out selected
        with open(outpath, 'w') as f:
            f.write('\n'.join([json.dumps(tweet) for tweet in selected]))
        #with open(csv_outpath, 'w') as f:
        #    df = pd.json_normalize(selected)
        #    if 'text' in df.columns:
        #        processed = process_tweets(df)
        #        processed.to_csv(csv_outpath)

    def build_vocab(self, fpath):
        """ Build unigram vocabulary from bios in a JSON tweet dump, save out as json """
        vocab = set()
        fname = os.path.basename(fpath)
        outpath = os.path.join('../output', 'vocab', f'{fname.split(".")[0]}.json')
        #csv_outpath = os.path.join('../output', 'tweets_csv', f'{fname.split(".")[0]}.csv')
        if os.path.exists(outpath):
            return

        #tqdm.write(fname)
        with gzip.open(fpath, 'rb') as f:
            for line in f:
                if len(line) == 1:
                    continue
                try:
                    tweet = json.loads(line)
                except json.decoder.JSONDecodeError:
                    tqdm.write('json decode error')
                    continue
                except UnicodeDecodeError:
                    tqdm.write('unicode decode error')
                    continue
                if 'user' in tweet and tweet['user']['description'] is not None:
                    vocab |= set([wd for wd in tweet['user']['description'].split()])

        # Save out dump's vocab (to later combine)
        with open(outpath, 'w') as f:
            json.dump(list(vocab), f)

    def tweet_dump_paths(self):
        """ Returns list of all COVID twitter dump paths """
        # Older data
        dirname = 'json_keyword_stream'
        paths = [os.path.join(self.basepath, dirname, fname) for fname in sorted(os.listdir(os.path.join(self.basepath, dirname)))]

        # Newer data
        dirname = 'json_keyword_stream_mike'
        paths += [os.path.join(self.basepath, dirname, fname) for fname in sorted(os.listdir(os.path.join(self.basepath, dirname))) if fname.endswith('json.gz')]

        return paths

    def run(self):
        n_cores = 20

        #csv_dirpath = os.path.join('../output', 'tweets_csv')
        json_dirpath = os.path.join('../output', 'tweets_json')
        if self.overwrite:
            #if os.path.exists(csv_dirpath):
            #    shutil.rmtree(csv_dirpath)
            #os.mkdir(csv_dirpath)
        if not os.path.exists(json_dirpath):
            os.mkdir(json_dirpath)

        input_paths = self.tweet_dump_paths()

        if not self.load_vocab:
            print("Building vocab...")
            with Pool(n_cores) as p:
                list(tqdm(p.imap(self.build_vocab, input_paths), ncols=80, total=len(input_paths)))
            # Combine vocabs
            vocab = set()
            vocab_dirpath = '../output/vocab/*'
            for path in tqdm(glob(vocab_dirpath)):
                vocab |= set(json.load(path))
            print(f"\tBuilt unigram vocab of {len(vocab)} words")

        print("Extracting identities...")
        self.load_identities(vocab)
        with Pool(n_cores) as p:
            list(tqdm(p.imap(self.process_dump, paths), ncols=80, total=len(input_paths)))
        #list(map(self.process_dump, paths)) # debugging

        #self.load_process_tweets()
    
        #print(Counter([select['search_match'] for select in selected]).most_common())


if __name__ == '__main__':
    tweet_filter = IdentityExtractor(load_vocab=False, overwrite=True)
    tweet_filter.run()
