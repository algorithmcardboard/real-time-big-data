patterns = LOAD 'words' AS (pat:chararray);
in = LOAD 'input';


Tokens = FOREACH in GENERATE TOKENIZE((chararray)$0) as word;
Tokens = FOREACH Tokens GENERATE FLATTEN(word) as word;
Tokens = FOREACH Tokens GENERATE FLATTEN(REGEX_EXTRACT_ALL(LOWER(word),'.*?([a-z]+).*?')) AS word;

inter_res = JOIN patterns BY pat LEFT, Tokens BY $0 ;

temp1  = GROUP inter_res BY patterns::pat;

actual_result  = FOREACH temp1 GENERATE $0, COUNT($1.Tokens::word);

DUMP actual_result;
