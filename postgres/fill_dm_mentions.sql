CREATE OR REPLACE PROCEDURE public.fill_dm_mentions(i_start_date date DEFAULT ((now())::date - 2), i_end_date date DEFAULT (now())::date)
 LANGUAGE plpgsql
AS $procedure$

BEGIN

   DELETE FROM public.dm_mentions
   where news_date between i_start_date AND i_end_date
   ;

   WITH wt_syn AS
   /* Для повторной проверки синонимов */
   (
     SELECT e.entity_id ,
            e2.entity_id AS synonim_entity_id
     FROM public.ref_synonyms s
     JOIN public.entity e ON e.entity_name = upper(s.entity_nm)
     JOIN public.entity e2 ON e2.entity_name = upper(s.synonim_entity_nm)
   ),
   wt_ex  AS
   /* для повторной проверки исключений */
   (
     SELECT e.entity_id
     FROM public.ref_exeptions re
     JOIN public.entity e ON e.entity_name = upper(re.entity_name)
   ),
   wt_src AS
   (
       SELECT n.news_date,
              n.news_src_id,
              coalesce(wt_syn.entity_id, e.entity_id) AS entity_id,
              e.entity_type_id
       FROM public.news n
       JOIN public.news_x_entity nxe ON nxe.news_id = n.news_id
       JOIN public.entity e ON e.entity_id = nxe.entity_id
       /* Повторная проверка синонимов для возможности перерасчета за архивные даты
        * на случай добавления синонимов "задним числом" */
       LEFT JOIN wt_syn ON wt_syn.synonim_entity_id = e.entity_id
       WHERE n.news_date between i_start_date AND i_end_date
       /* Повторная проверка исключений для возможности перерасчета за архивные даты
        * на случай добавления синонимов "задним числом" */
         AND coalesce(wt_syn.entity_id, e.entity_id) NOT IN (SELECT entity_id FROM wt_ex)

   )
   INSERT INTO public.dm_mentions (entity_id, news_date, news_src_id, mention_qnt, entity_type_id)
   SELECT entity_id,
          news_date,
          news_src_id,
          count(1) AS mention_qnt,
          entity_type_id
     FROM wt_src
     GROUP BY news_date, news_src_id, entity_id, entity_type_id
   ;

   COMMIT
   ;

END;
$procedure$
;
