CREATE OR REPLACE PROCEDURE public.fill_dm_mentions_stat(i_start_date date DEFAULT ((now())::date - 2), i_end_date date DEFAULT (now())::date)
 LANGUAGE plpgsql
AS $procedure$

DECLARE i integer;
        N integer DEFAULT (i_end_date - i_start_date);

BEGIN

   DELETE FROM public.dm_mentions_stat
   where actual_date between i_start_date AND i_end_date
    ;

FOR i IN 0 .. n

    LOOP
        WITH
        wt_day AS  -- dayly data
        (
            SELECT d.entity_id,
                   d.entity_type_id,
                   d.news_src_id,
                   d.mention_qnt AS qnt,
                   DENSE_RANK() OVER (PARTITION BY d.entity_type_id, d.news_src_id order BY d.mention_qnt desc) rnk
            FROM public.dm_mentions d
            WHERE d.news_date = i_start_date + i
        ),
        wt_week AS -- weekly data
        (
          SELECT entity_id,
                 entity_type_id,
                 news_src_id,
                 qnt,
                 DENSE_RANK() OVER (PARTITION BY entity_type_id, news_src_id order BY qnt desc) rnk
           FROM
            (
              SELECT d.entity_id,
                     d.entity_type_id,
                     d.news_src_id,
                     SUM (d.mention_qnt) AS qnt
               FROM public.dm_mentions d
               WHERE d.news_date BETWEEN  i_start_date + i - 7 AND i_start_date + i
               GROUP BY entity_id, entity_type_id, news_src_id
            ) t
        ),
        wt_month AS -- monthly data
        (
          SELECT entity_id,
                 entity_type_id,
                 news_src_id,
                 qnt,
                 DENSE_RANK() OVER (PARTITION BY entity_type_id, news_src_id order BY qnt desc) rnk
           FROM
            (
              SELECT d.entity_id,
                     d.entity_type_id,
                     d.news_src_id,
                     SUM (d.mention_qnt) AS qnt
               FROM public.dm_mentions d
               WHERE d.news_date BETWEEN  i_start_date + i - 30 AND i_start_date + i
               GROUP BY entity_id, entity_type_id, news_src_id
            ) t
        ),
        wt_src AS
        (
            SELECT i_start_date + i AS actual_date,
                   m.entity_id,
                   m.entity_type_id,
                   m.news_src_id,
                   COALESCE(d.qnt, 0) AS daily_qnt,
                   COALESCE(d.rnk, 0) AS daily_rank,
                   COALESCE(w.qnt, 0) AS weekly_qnt,
                   COALESCE(w.rnk, 0) AS weekly_rank,
                   m.qnt AS monthly_qnt,
                   m.rnk AS monthly_rank
              FROM wt_month m
              JOIN wt_week w ON w.entity_id = m.entity_id AND
                                w.news_src_id = m.news_src_id AND
                                w.entity_type_id = m.entity_type_id
              JOIN wt_day d ON d.entity_id = m.entity_id AND
                               d.news_src_id = m.news_src_id AND
                               d.entity_type_id = m.entity_type_id
              ORDER BY monthly_rank
        )
        INSERT INTO public.dm_mentions_stat (actual_date, entity_id, entity_type_id, news_src_id,
                                             daily_qnt, daily_rank, weekly_qnt, weekly_rank, monthly_qnt, monthly_rank)
        SELECT actual_date,
               entity_id,
               entity_type_id,
               news_src_id,
               daily_qnt,
               daily_rank,
               weekly_qnt,
               weekly_rank,
               monthly_qnt,
               monthly_rank
         FROM wt_src
         ;
         COMMIT;

     END LOOP;

END;
$procedure$
;
