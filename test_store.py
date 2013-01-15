import os
from save import Manager


if __name__ == "__main__":
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews"
    tweetsDir = os.path.join(mainDir, "stream", "tweets")
    mgr = Manager(tweetsDir)
    stream = mgr.restore(lastOnly=True)
    all = 0
    text = 0
    url = 0
    for t in stream:
        all += 1
        if 'text' in t:
            text += 1
            entities = t['entities']
            if 'urls' in entities and entities['urls'] or 'media' in entities and entities['media']:
                url+=1

    print("ALL: %d TEXT: %d URL: %d" % (all, text, url))

