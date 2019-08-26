with extraced_words as (
  SELECT id, REGEXP_EXTRACT_ALL(lower(message), r"[\w]+") words
    FROM `dataengineerproject-250819.DataEngineerDataset.posts` 
  ),
  stop_words as(
    SELECT stop_word 
      FROM UNNEST(['the','and','you','i','to','of','a','my']) stop_word
  )
select id, word, count(*) word_count from extraced_words, unnest(extraced_words.words) as word
  where word not in (select stop_word from  stop_words)
  group by 1,2
