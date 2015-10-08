options(scipen=666)
library(stringr)
library(jsonlite)

tx = paste0(readLines('ohsumed.87.txt'), collapse=' ')
tx = gsub('<COLLECTION>|</COLLECTION>', '', tx)
tx = gsub('\t{1,}', ' ', tx)

docs = strsplit(tx, '<DOC>')
docs = docs[[1]]
rm(tx);gc()

parse_trec = function(i){
  x = docs[[i]]
  
  fields = unlist(str_extract_all(x, '<[A-Z]{1,}>'))
  text_in_tag <- function(tag, x){
    closing_tag = gsub('<', '</', tag)
    content = gsub(sprintf("%s.*", closing_tag), '', x)
    content = gsub(sprintf(".*%s", tag), '', content)
    content = gsub(' {1,}', ' ', content)
    content = gsub(':', '', content)
    trimws(content)
  }
  
  x_fields = lapply(fields, function(tag) {
    text_in_tag(tag, x)
  })
  
  clean_fields = gsub('[[:punct:]]', '', fields)
  names(x_fields) = tolower(clean_fields)
  
  if(clean_fields[[1]] == 'id'){
    x_fields[[1]] = as.numeric(x_fields[[1]])
  }
  
  if(clean_fields[[2]] == 'docno'){
    x_fields[[2]] = as.numeric(x_fields[[2]])
  }
  
  x_fields = gsub('\\]|\\[', '', toJSON(x_fields))
  x_fields
}

plosdat <- system.file("examples", "plos_data.json", package = "elastic")


fileConn <- file("clean_corpus.txt", 'w')
for(i in 2:10){
  iter = i - 1
  index_line = sprintf('{"index":{"_index":"doc","_type":"papers","_id":%s}}', iter)
  print(index_line)
  content = parse_trec(i)
  writeLines(c(index_line,content), con=fileConn, sep='\n')
}
close(fileConn)

system('cat clean_corpus.txt')

library(elastic)
library(jsonlite)

connect()
docs_bulk('clean_corpus.txt')

docs_mget(index='doc', type='papers', id=1:5, fields='id')



