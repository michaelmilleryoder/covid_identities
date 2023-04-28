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
import pickle
import itertools

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
    all_matches = list(re.finditer(identity_pat, str(text).lower()))
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


def term_in_vocab(term, vocab):
    """ Returns if a term is in the vocab """
    # TODO: change to only return multi-word terms if all unigrams are in vocab
    unigrams = term.lower().split()
    for wd in unigrams:
        if wd in vocab:
            return term
    else:
        return None


class IdentityExtractor():

    def __init__(self, load_vocab=False, vocab_path='../tmp/vocab.pkl', pat_path='../tmp/pat.pkl', overwrite=False, n_cores=1):
        """ Args:
            load_vocab: If False, will build unigram vocab from all the Twitter dumps
            vocab_path: path to load vocab from (if load_vocab is True) or save vocab to (if load_vocab is False).
                Default is ../tmp/vocab.pkl. Will save to pickle
            overwrite: If True, will overwrite any existing files already saved out from extraction
            n_cores: number of processors to use
        """
        self.load_vocab = load_vocab
        self.vocab_path = vocab_path
        self.pat_path = pat_path
        self.overwrite = overwrite
        self.n_cores = n_cores
        self.identities_path = '../resources/generic_agents-identity_v26_Netanomics.xlsx'
        self.identities = None
        self.basepath = '/storage3/coronavirus/'
        self.paths = None
        self.n_selected = 0
        self.identity_pat = None
        self.vocab = None

    def load_identities(self):
        """ Load, filter identities to just those that are present in the vocab
        """
        if os.path.exists(self.pat_path) and self.load_vocab and not self.overwrite:
            print("Loading identity list...")
            with open(self.pat_path, 'rb') as f:
                self.identity_pat = pickle.load(f)
            print(f"\tLoaded identities regex pattern from {self.pat_path}")

        else: # create pattern
            print("Filtering identity list...")
            identities = pd.read_excel(self.identities_path)
            identities['english'] = identities['English'].str.lower()
            identities = identities.drop_duplicates('english')['english'].tolist()
            #unigram_identities = [term for term in identities if ' ' not in term]
            #mw_identities = [term for term in identities if ' ' in term]

            filtered = [term_in_vocab(term, self.vocab) for term in tqdm(identities, ncols=80)] # for debugging
            filtered = [term for term in filtered if term is not None]
            #self.identities = unigram_identities + mw_identities
            self.identities = filtered
            print(f"\t{len(self.identities)} present in vocabulary out of {len(identities)}")
            self.identity_pat = re.compile(r'|'.join([r'\b{}\b'.format(re.escape(term)) for term in self.identities]))

            # Save out filtered identities
            filtered_outpath = '../tmp/filtered_identities.json'
            with open(filtered_outpath, 'w') as f:
                json.dump(self.identities, f, indent=4)
            print(f"\tSaved filtered identities to {filtered_outpath}")

            # Save out pattern since took a long time to load
            with open(self.pat_path, 'wb') as f:
                pickle.dump(self.identity_pat, f)
            print(f"\tSaved identity regex patterns to {self.pat_path}")

    def select_tweet(self, tweet):
        """ See if a tweet is worth keeping (matches enough criteria).
            Extract identities from user bio if tweet does match criteria.
        """
        matches, spans = None, None
        
        # Basic cleaning
        if len(tweet) == 1 and 'limit' in tweet:
            return matches, spans
        
        # Language is English
        # TODO: change to checking for user language
        #if tweet['lang'] != 'en':
        #    return select

        # Check for containing a user description
        if not 'user' in tweet:
            return matches, spans
        if tweet['user']['description'] is None:
            return matches, spans
        
        # Contains identity terms
        #if 'extended_tweet' in tweet:
        #    text = tweet['extended_tweet']['full_text'].lower()
        #else:
        #    text = tweet['text'].lower()
        matches, spans = match_identities(tweet['user']['description'], self.identity_pat)
        #for p in self.pats:
        #    m = re.search(p, text)
        #    # if any([re.search(p, tweet['extended_tweet']['full_text'].lower()) for p in pats]):
        #    if m is not None:
        #        select = m.group()
        #        # tqdm.write('one selected')
                
        return matches, spans

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
        tqdm.write(fname)
        outpath = os.path.join('../output', 'tweets_json', f'{fname.split(".")[0]}.jsonl')
        #csv_outpath = os.path.join('../output', 'tweets_csv', f'{fname.split(".")[0]}.csv')
        if os.path.exists(outpath) and not self.overwrite: # already processed
            return

        # Build set of unique bios, just extract on those
        bios = set()
        # TODO: save out username and possibly timestamp of first occurrence as well (probably as dicts)
        #limit = 100
        #ctr = 0
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
                if not 'user' in tweet:
                    continue
                bios.add(tweet['user']['description'])
                #ctr += 1
                #if ctr == 100:
                #    break
        #matches_spans = [match_identities(bio, self.identity_pat) for bio in tqdm(bios, ncols=80)]
        matches_spans = [match_identities(bio, self.identity_pat) for bio in bios]
        matches, spans = list(zip(*matches_spans))
        bio_matches = pd.DataFrame(list(zip(bios, matches, spans)), columns=['bio', 'identities', 'identity_spans'])
        extracted = bio_matches[bio_matches['identities'].map(lambda x: len(x) > 1)]
        # Save out bio matches
        extracted.to_json(outpath, orient='records', lines=True)

        # Run extraction on every tweet
        #with gzip.open(fpath, 'rb') as f:
        #    #for i, line in tqdm(enumerate(f), total=974483):
        #    # for i, line in tqdm(enumerate(f), total=974483, bar_format='selected: {postfix} | Elapsed: {elapsed} | {rate_fmt}', postfix=n_selected):
        #    for line in f:
        #        if len(line) == 1:
        #            continue
        #        try:
        #            tweet = json.loads(line)
        #        except json.decoder.JSONDecodeError:
        #            tqdm.write('json decode error')
        #            continue
        #        except UnicodeDecodeError:
        #            tqdm.write('unicode decode error')
        #            continue
        #        match = self.select_tweet(tweet)
        #        if match:
        #            tweet['search_match'] = match
        #            selected.append(tweet)
        #        # if i > 100:

        ## Save out selected
        #with open(outpath, 'w') as f:
        #    f.write('\n'.join([json.dumps(tweet) for tweet in selected]))
        ##with open(csv_outpath, 'w') as f:
        ##    df = pd.json_normalize(selected)
        ##    if 'text' in df.columns:
        ##        processed = process_tweets(df)
        ##        processed.to_csv(csv_outpath)

    def build_vocab(self, fpath):
        """ Build unigram vocabulary from bios in a JSON tweet dump, save out as json """
        vocab = set()
        fname = os.path.basename(fpath)
        outpath = os.path.join('../output', 'vocab', f'{fname.split(".")[0]}.json')
        #csv_outpath = os.path.join('../output', 'tweets_csv', f'{fname.split(".")[0]}.csv')
        if os.path.exists(outpath) and not self.overwrite:
            return

        #tqdm.write(fname)
        # pandas way
        #bios = []
        #with gzip.open(fpath, 'rb') as f:
        #    for line in f:
        #        if len(line) == 1:
        #            continue
        #        try:
        #            tweet = json.loads(line)
        #        except json.decoder.JSONDecodeError:
        #            tqdm.write('json decode error')
        #            continue
        #        except UnicodeDecodeError:
        #            tqdm.write('unicode decode error')
        #            continue
        #        if 'user' in tweet and tweet['user']['description'] is not None:
        #            bios.append(tweet['user']['description'])
        #pd.Series(bios).str.lower().str.split().apply(vocab.update) # TODO: probably could get rid of Series overhead

        # Line-by-line way
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
                    vocab.update(tweet['user']['description'].lower().split())

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
        #csv_dirpath = os.path.join('../output', 'tweets_csv')
        json_dirpath = os.path.join('../output', 'tweets_json')
        #if self.overwrite:
            #if os.path.exists(csv_dirpath):
            #    shutil.rmtree(csv_dirpath)
            #os.mkdir(csv_dirpath)
        if not os.path.exists(json_dirpath):
            os.mkdir(json_dirpath)

        input_paths = self.tweet_dump_paths()

        if self.load_vocab:
            print("Loading vocab...")

            # Load vocab
            with open(self.vocab_path, 'rb') as f:
                self.vocab = pickle.load(f)

        else:
            print("Building vocab...")
            with Pool(self.n_cores) as p:
                list(tqdm(p.imap(self.build_vocab, input_paths), ncols=80, total=len(input_paths)))
            #list(map(self.build_vocab, input_paths)) # debugging

            # Combine vocabs
            self.vocab = set()
            vocab_dirpath = '../output/vocab/*'
            print("\tCombining vocabs...")
            for path in tqdm(glob(vocab_dirpath), ncols=80):
                #tqdm.write(path)
                with open(path) as f:
                    self.vocab.update([w.lower() for w in json.load(f)])
            print(f"\tBuilt unigram vocab of {len(self.vocab)} words")

            # Save out vocab
            with open(self.vocab_path, 'wb') as f:
                pickle.dump(self.vocab, f)

        print("Extracting identities...")
        self.load_identities()
        print("\tMatching identities...")
        with Pool(self.n_cores) as p:
            list(tqdm(p.imap(self.process_dump, input_paths), ncols=80, total=len(input_paths)))
        #list(map(self.process_dump, input_paths)) # debugging

        #self.load_process_tweets()
    
        #print(Counter([select['search_match'] for select in selected]).most_common())


if __name__ == '__main__':
    tweet_filter = IdentityExtractor(load_vocab=True, overwrite=False, n_cores=25)
    tweet_filter.run()
