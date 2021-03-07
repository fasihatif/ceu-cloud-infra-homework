## SUBJECT DATE
DATE_PARAM="2021-01-26"

date <- as.Date(DATE_PARAM, "%Y-%m-%d")

#> install.packages('httr', 'jsonlite', 'lubridate')
library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)

# See https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/2021/03/01
url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/",
  format(date, "%Y/%m/%d"), sep='')

wiki.server.response = GET(url)
wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')


# Save Raw Response and upload to S3
RAW_LOCATION_BASE='ceu-data-platform-in-the-cloud-class/data/raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = TRUE)


# Write file name
filename = paste("raw-views-", format(date, "%Y-%m-%d"),sep = '')
filepath = paste(RAW_LOCATION_BASE,'/',filename, sep = '')
write(wiki.response.body, filepath)


### Upload the file you created to S3 into your bucket into an object called de4/raw/raw-views-YYYY-MM-DD.txt

# Setting up AWS Access

keyTable <- read.csv("ceu-data-platform-in-the-cloud-class/accessKeys.csv", header = T) # accessKeys.csv == the CSV downloaded from AWS containing your Acces & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

#activate
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 

# Upload to S3

put_object(file = filepath,
           object = filename,
           bucket = "fasih.atif/de4/raw/",
           verbose = TRUE)


### Convert the response into a JSON lines formatted file (as we did in the class)

# Parse the response and write the parsed string to "Bronze"

# We are extracting the top edits from the server's response
wiki.response.parsed = content(wiki.server.response, 'parsed')
top.articles = wiki.response.parsed$items[[1]]$articles


# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (page in top.articles){
  record = list(
    article = page$article,
    views = page$views,
    rank = page$rank,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Save the Top Edits JSON lines as a file and upload it to S3

# Write the file to your computer to data/views/views-YYYY-MM-DD.json

JSON_LOCATION_BASE='ceu-data-platform-in-the-cloud-class/data/views'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = FALSE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

## Upload the JSON lines file you saved locally to S3 into your bucket into an object called de4/views

put_object(file = json.lines.fullpath,
           object = json.lines.filename,
            bucket = "fasih.atif/de4/views",
            verbose = TRUE)


