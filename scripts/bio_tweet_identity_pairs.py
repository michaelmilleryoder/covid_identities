""" Calculate bio and tweet identity pairs over time
    Outputs monthly counts of identity pairs that occur in bios and in tweets
"""

import os
import pdb
from glob import glob
from collections import Counter
import itertools

import pandas as pd
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
tqdm.pandas()


def extract_pairs_dump_star(args):
    extract_pairs_dump(*args)


def extract_pairs_dump(dump_fpath, out_dirpath, vocab_n, bio_stops, tweet_stops):
    """ Extract bio and tweet identity pair counts from one dump file """
    fname = os.path.basename(dump_fpath)
    out_fpath = os.path.join(out_dirpath, fname)
    if os.path.exists(out_fpath):
        return

    # Load bio and tweet-identified files
    data = pd.read_json(dump_fpath, lines=True)
    if 'identities' not in data.columns: # probably empty df
        return
    
    # Calculate most frequent 1000 identities in bios and most frequent in 1000 tweets
    data['identities'] = data['identities'].map(lambda x: [w for w in x if w not in bio_stops])
    data['tweet_identities'] = data['tweet_identities'].map(lambda x: [w for w in x if w not in tweet_stops])
    bio_ctr = Counter()
    tweets_ctr = Counter()
    data.identities.map(bio_ctr.update)
    data.tweet_identities.map(tweets_ctr.update)
    top_bio_identities, _ = zip(*bio_ctr.most_common(vocab_n))
    top_tweet_identities, _ = zip(*tweets_ctr.most_common(vocab_n))
    
    data['identities'] = data['identities'].map(lambda x: sorted(set([w for w in x if w in top_bio_identities])))
    data['tweet_identities'] = data['tweet_identities'].map(lambda x: [w for w in x if w in top_tweet_identities])
    
    # Remove rows if don't have at least one top identity in bios and in tweet
    data = data[(data['identities'].map(lambda x: len(x) > 0)) & (data['tweet_identities'].map(lambda x: len(x) > 0))]

    # Group by unique users
    data_user = data.groupby('user.id_str').agg({
    'identities': 'first',
    'tweet_identities': lambda x: sorted(set([identity for identities in x for identity in identities]))
    })

    # Calculate counts of bio-tweet identity pairs
    tweet_bio_ctr = Counter()
    data_user.apply(lambda row: tweet_bio_ctr.update(
        itertools.product(row['identities'], row['tweet_identities'])), axis=1)

    most_common = tweet_bio_ctr.most_common()
    counts_df = pd.DataFrame([{'bio_identity': el[0][0], 'tweet_identity': el[0][1],
               'user_count': el[1]} for el in most_common])
    counts_df['date'] = data.created_at.dt.date.unique()[0].strftime("%Y-%m-%d")
    counts_df.to_json(out_fpath, orient='records', lines=True)
    #tqdm.write(f'Wrote to {out_fpath}')


class BioTweetIdentityPairer:

    def __init__(self, vocab: int):
        """ Args:
                vocab: number of most frequent identities to consider 
                    for both bios and tweets (separately)
        """
        self.vocab_n = vocab
        # path to files with identities in bios and tweets already extracted
        self.extracted_path = '../output/tweets_bios_identities' 
        self.out_dirpath = f'../output/bio_tweet_identity_pairs_{self.vocab_n}/'
        if not os.path.exists(self.out_dirpath):
            os.mkdir(self.out_dirpath)
        self.bio_stops = ['i', 'you', 'us', 'we', 'my', 'me', 'it', 'your', 'our', 'who', 'its', 'those', 'other', 'everyone', 
                    'people', 'don']
        self.tweet_stops = self.bio_stops + ['they', 'he', 'him', 'his', 'their', 'them', 'she', 'her', 'hers', 'theirs', 'person']

    def extract_pairs(self):
        """ Extract top identity pairs in bios and tweets, save out """
        tweet_fpaths = sorted(glob(os.path.join(self.extracted_path, '*')))
        zipped = list(zip(tweet_fpaths, itertools.repeat(self.out_dirpath), itertools.repeat(self.vocab_n), itertools.repeat(self.bio_stops), itertools.repeat(self.tweet_stops)))
        process_map(extract_pairs_dump_star, zipped, max_workers=25, ncols=80, total=len(tweet_fpaths))
        #list(map(extract_pairs_dump_star, zipped)) # debugging


def main():
    identity_pairer = BioTweetIdentityPairer(vocab=1000)
    identity_pairer.extract_pairs()


if __name__ == '__main__':
    main()
