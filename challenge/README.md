# Bootcamp: UCB-VIRT-DATA-PT-03-2020-U-B-TTH
## Bootcamp Challenge #8 - 5/3/2020
Bootcamp Challenge 8: Module movies-etl

### Dataset Used
- [wikipedia.movies.json](https://courses.bootcampspot.com/courses/140/files/37183/download?wrap=1)
- [zip file from Kaggle](https://www.kaggle.com/rounakbanik/the-movies-dataset/download)

### Challenge Objectives
The goals of this challenge are for you to:
- Create an automated ETL pipeline.
- Extract data from multiple sources.
- Clean and transform the data automatically using Pandas and regular expressions.
- Load new data into PostgreSQL.

### Challenge Findings
#### Box Office
![Box Office](./analysis/box_office.png)
#### Budget
![Budget](./analysis/budget_wiki_kaggle.png)
#### Ratings Frequency
![Ratings](./analysis/ratings_frequency.png)
#### Release Date
![Release Date](./analysis/release_date.png)
#### Revenue
![Revenue](./analysis/revenue.png)
#### Running Time
![Running Time](./analysis/running_time.png)

### PostGresSQL Table CSV exports.
[Movies Table Export](./db/movies_dump.csv)
</br>
[Ratings Table Export](./db/ratings_dump.csv)
</br>

### Assumptions Identified:
1. imdb_id Format: String format of the URL to extract imdb_id we utilize this regular expression: r'(tt\d{7})'. This searches for a string that begins with 'tt' and is followed by 7 digits. If there is another string in the URL that also starts with tt and is followed by 7 digits, the RegEx will take the 1st match. e.g. if the URL was https://www.imdb.com/title/tt7654321/tt1234567/, the RegEx would return 7654321 not 1234567. Code assumes only 1 instance of this tt followed by 7 digits string.
2. Additionally it is assumed that the length of the imdb_id is always exactly 7 length, if it increases to 8 digits, we would not capture the full number. e.g. https://www.imdb.com/title/tt7654321/tt12345678/ would have a full id of 12345678 but the RegEx would return only 1234567.
3. Numeric String for Kaggle Metadata ID field - When setting the kaggle_metadata['id'] field, we use pd.to_numeric function which will result in an error if the data present is not a numeric value in the String data type. If the value present is just text and not numeric, the code would throw a ValueError; I added a try except clause to this code.
4. Numeric String for Kaggle Metadata Popularity field - When setting the kaggle_metadata['id'] field, we use pd.to_numeric function which will result in an error if the data present is not a numeric value in the String data type. If the value present is just text and not numeric, the code would throw a ValueError; I added a try except clause to this code.
5. Running Time string format: When extracting the running time, we do not take into account days or minutes. Thus if an expected string comes such as 2h 5m the code will properly capture it and convert the time to 125 minutes. If however the string is passed such as 1d 1h 1m 1s, the RegEx would properly detect the hours and minutes and return 71min when in fact we should have captured the 1 day and 1 second with true value expected of 60*24 (1d) + 60 (1h) + 1 (1m) + 1/60 (1s), expected (1,501 1/60). RegEx used: r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m'. 
6. It is assumed that the wiki_movies.json file is indeed of well formed JSON structure as the code initially reads the JSON and uses it to create a new dataframe. If the file does not contain JSON, it will throw a ValueError and thus I added a try except block specifically for this use case.
7. This script assumes that there are multiple files present on disk in the specified path, if the files are not present the script would throw a FileNotFoundError exception. As we read the three files in the beginning, I wrapped the file reading block in a try except to process this exception. 
