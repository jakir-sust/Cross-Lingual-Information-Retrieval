
import demoji, re, datetime
import emoji
import preprocessor

def remove_emoji(text):
    return emoji.get_emoji_regexp().sub(u'', text)


# demoji.download_codes()


class TWPreprocessor:
    @classmethod
    def preprocess(cls, tweet, type):
        '''
        Do tweet pre-processing before indexing, make sure all the field data types are in the format as asked in the project doc.
        :param tweet:
        :return: dict
        '''

        tweet_field = {}
        tweet_field['id'] = tweet.id
        if tweet.lang == 'es':
            tweet_field['country'] = 'Mexico'
        elif tweet.lang == 'en':
            tweet_field['country'] = 'USA'
        else:
            tweet.lang = 'hi'
            tweet_field['country'] = 'India'

        tweet_field['tweet_lang'] = tweet.lang
        tweet_field['tweet_text'] = tweet.full_text

        text, emojis = _text_cleaner(tweet.full_text)
        if tweet_field['country'] == 'India':
            text = remove_emoji(tweet.full_text)
        text_xx = 'text_' + tweet.lang
        tweet_field[text_xx] = text

        tweet_field['tweet_date'] = str(tweet.created_at)
        tweet_field['verified'] = tweet.user.verified

        if type == 'poi':
            tweet_field['poi_id'] = tweet.user.id
            tweet_field['poi_name'] = tweet.user.screen_name
        tweet_field['replied_to_tweet_id'] = tweet.in_reply_to_status_id
        tweet_field['replied_to_user_id'] = tweet.in_reply_to_user_id

        if tweet.in_reply_to_user_id == None:
            tweet_field['reply_text'] = None
        else:
            tweet_field['reply_text'] = tweet_field['tweet_text']

        tweet_field['hashtags'] =_get_entities(tweet, 'hashtags')
        tweet_field['mentions'] = _get_entities(tweet, 'mentions')
        tweet_field['tweet_urls'] = _get_entities(tweet, 'urls')
        tweet_field['tweet_emoticons'] = emojis
        if tweet.geo != None:
            tweet_field['geolocation'] = tweet.geo['coordinates']

        #print("Preprocess done")
        #print(tweet_field)

        return tweet_field

        raise NotImplementedError


def _get_entities(tweet, type=None):
    result = []
    if type == 'hashtags':
        hashtags = tweet.entities['hashtags']

        for hashtag in hashtags:
            result.append(hashtag['text'])
    elif type == 'mentions':
        mentions = tweet.entities['user_mentions']

        for mention in mentions:
            result.append(mention['screen_name'])
    elif type == 'urls':
        urls = tweet.entities['urls']

        for url in urls:
            result.append(url['url'])

    return result


def _text_cleaner(text):
    emoticons_happy = list([
        ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
        ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
        '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
        'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
        '<3'
    ])
    emoticons_sad = list([
        ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
        ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
        ':c', ':{', '>:\\', ';('
    ])
    all_emoticons = emoticons_happy + emoticons_sad

    emojis = list(demoji.findall(text).keys())
    clean_text = demoji.replace(text, '')

    for emo in all_emoticons:
        if (emo in clean_text):
            clean_text = clean_text.replace(emo, '')
            emojis.append(emo)

    clean_text = preprocessor.clean(text)
    # preprocessor.set_options(preprocessor.OPT.EMOJI, preprocessor.OPT.SMILEY)
    # emojis= preprocessor.parse(text)

    return clean_text, emojis


def _get_tweet_date(tweet_date):
    return _hour_rounder(datetime.datetime.strptime(tweet_date, '%a %b %d %H:%M:%S +0000 %Y'))


def _hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
            + datetime.timedelta(hours=t.minute // 30))
