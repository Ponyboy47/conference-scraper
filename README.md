# General Conference Scraper

Scrapes [the Church of Jesus Christ of Latter-Day Saints website](https://www.churchofjesuschrist.org/?lang=eng) to
generate a JSON + SQLite database of conference talks.

Forked from: https://github.com/lukejoneslj/GeneralConferenceScraper/blob/main/ConferenceScraper.ipynb

## SQLite Schema

|table|description|
|-|-|
|speakers|Full names of conference speakers|
|organization|Church Organizations|
|callings|Callings from conference speakers|
|conferences|Year and season of General Conferences|
|talks|Messages delivered at General Conference|
|talk_speakers|The speaker who delivered a conference talk|
|talk_conferences|The conference when a talk was delivered|
|talk_callings|The calling of the speaker at the time of a conference talk|
|talk_texts|The textual content of a conference talk|
|talk_urls|URLs linking a talk to an audio, visual, or textual representation|
|talk_topics|Topics included in the message of a given talk|

### Tables

#### Speakers

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|name|text|The full name of a conference speaker|

#### Organizations

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|name|text|The name of the church organization|

#### Callings

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|name|text|The full calling name|
|organization|integer|The organization foreign key associated with the calling|
|rank|integer|How many potential steps below "prophet" is the calling (not an exact church-sanctioned hierarchy)|

#### Conferences

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|year|integer|Which year was the conference held|
|season|text|Was it the April or October conference|

#### Talks

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|title|text|The speaker's designated name for their conference remarks|
|emeritus|integer|Whether or not the speaker was technically released at the time of giving the talk (0 if false, anything else == true)|
|conference|integer|The conference foreign key associated with the talk|

#### Talk Speakers

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this text corresponds|
|speaker|integer|The speaker foreign key associated with the talk|

#### Talk Callings

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this text corresponds|
|calling|integer|The calling foreign key associated with the speaker at the time of the talk|

#### Talk Texts

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this text corresponds|
|text|text|The actual text of the message|

#### Talk URLs

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this url corresponds|
|url|text|The actual URL assocated with the talk|
|kind|text|The type of data expected at the URL (audio, video, or text)|

#### Talk Topics

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this topic corresponds|
|name|text|The name of the topic|

## Contributing

Check out [CONTRIBUTING.md](CONTRIBUTING.md)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
