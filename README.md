# General Conference Scraper

Scrapes [the Church of Jesus Christ of Latter-Day Saints website](https://www.churchofjesuschrist.org/?lang=eng) to
generate a JSON + SQLite database of conference talks.

Forked from: https://github.com/lukejoneslj/GeneralConferenceScraper/blob/main/ConferenceScraper.ipynb

## Usage

Make sure you have [`uv` installed](https://docs.astral.sh/uv/getting-started/installation/). Then simply:

```sh
uv run conference-scraper
```

Afterwards check the newly created `data` directory for an up-to-date conference_talks.json, conference_talks.db, conference_talks_no_text.db.

Or you can just get the latest ones already generated in the [releases](https://github.com/Ponyboy47/conference-scraper/releases)

## SQLite Schema

|table/view|description|
|-|-|
|speakers|Full names of conference speakers|
|organization|Church Organizations|
|callings|Callings from conference speakers|
|conferences|Year and season of General Conferences|
|sessions|Session name of a General Conference portion|
|talks|Messages delivered at General Conference|
|talk_speakers|The speaker who delivered a conference talk|
|talk_conferences|The conference when a talk was delivered|
|talk_sessions|The session when a talk was delivered|
|talk_callings|The calling of the speaker at the time of a conference talk|
|talk_texts|The textual content of a conference talk|
|talk_urls|URLs linking a talk to an audio, visual, or textual representation|
|talk_topics|Topics included in the message of a given talk|
|talk_details|Aggregated data for a talk included in a single easy-to-consume view|

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

#### Sessions

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|name|text|The name of the conference session|

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

#### Talk Sessions

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this text corresponds|
|sessions|integer|The sessions foreign key associated with the talk|

#### Talk Topics (optional)

> This one is not included in the files generated automatically for the releases.

|column|type|description|
|-|-|-|
|id|integer|Primary key|
|talk|integer|The talk foreign key to which this topic corresponds|
|name|text|The name of the topic|

### Views

#### Talk Details

|column|type|description|
|-|-|-|
|id|integer|Primary key (the talk's ID)|
|title|text|The title of the talk|
|emeritus|integer|Whether or not the speaker was technically released at the time of giving the talk (0 if false, anything else == true)|
|year|integer|Which year was the conference held|
|season|text|Was it the April or October conference|
|session|text|The name of the session when the talk was given|
|day|integer|The day of the week when the talk was given|
|speaker|text|The full name of the conference speaker|
|urls|text|Comma-separated list of URLs associated with the talk|
|calling|text|The full calling name of the speaker at the time of the talk|
|organization|text|The name of the church organization for the calling at the time of the talk|

## Contributing

Check out [CONTRIBUTING.md](CONTRIBUTING.md)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
