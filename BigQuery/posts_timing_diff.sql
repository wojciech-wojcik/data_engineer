with posts_ordered as
(SELECT id, created_time, row_number() over(order by created_time) num 
  FROM `dataengineerproject-250819.DataEngineerDataset.posts`)
select po1.id, po1.created_time creation_time,
  timestamp_diff(po1.created_time, po2.created_time, SECOND) seconds_from_last_post_creation 
  from posts_ordered po1
    left join posts_ordered po2
      on po1.num=po2.num+1
