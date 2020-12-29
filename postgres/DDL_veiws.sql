CREATE OR REPLACE VIEW public.vw_mentions
AS SELECT s.news_date,
    rns.news_src_name,
    ret.entity_type_name,
    e.entity_name,
    s.mention_qnt
   FROM dm_mentions s
     JOIN entity e ON s.entity_id = e.entity_id
     JOIN ref_entity_type ret ON ret.entity_type_id = s.entity_type_id
     JOIN ref_news_src rns ON rns.news_src_id = s.news_src_id
  WHERE 1 = 1;


  CREATE OR REPLACE VIEW public.vw_mentions_stat
AS SELECT s.actual_date,
    ret.entity_type_name,
    rns.news_src_name,
    e.entity_name,
    s.daily_qnt,
    s.daily_rank,
    s.weekly_qnt,
    s.weekly_rank,
    s.monthly_qnt,
    s.monthly_rank
   FROM dm_mentions_stat s
     JOIN entity e ON s.entity_id = e.entity_id
     JOIN ref_entity_type ret ON ret.entity_type_id = s.entity_type_id
     JOIN ref_news_src rns ON rns.news_src_id = s.news_src_id
  WHERE 1 = 1 AND s.actual_date = (( SELECT max(dm_mentions_stat.actual_date) - 1
           FROM dm_mentions_stat));