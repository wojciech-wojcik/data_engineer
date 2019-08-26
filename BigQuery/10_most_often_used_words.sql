with posts_word_count as
(SELECT word, sum(word_count) word_count FROM `dataengineerproject-250819.DataEngineerDataset.words`
  group by 1)
select pw.word, max(pw.word_count) word_count_posts,
  sh.corpus,
  max(sh.word_count) word_count_shakespeare from posts_word_count pw
left join (
    SELECT word, max(word_count) word_count, array_agg(corpus order by word_count desc)[offset(0)] corpus
      FROM `bigquery-public-data.samples.shakespeare` 
      group by 1
  )sh
  on pw.word = sh.word
  group by 1,3
  order by 2 desc
  limit 10
