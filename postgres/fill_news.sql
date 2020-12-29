CREATE OR REPLACE PROCEDURE public.fill_news()
 LANGUAGE plpgsql
AS $procedure$

BEGIN
    WITH src AS
    (
        SELECT DISTINCT
               cast(sn.news_datetime AS date) AS news_date,
               sn.news_src AS news_src_id,
               sn.news_uid AS news_external_uid
          FROM public.stg_news sn
    )
    INSERT INTO public.news (news_date, news_src_id, news_external_uid)
    SELECT news_date, news_src_id, news_external_uid
      FROM src
      WHERE NOT EXISTS (
                         SELECT 1 FROM public.news trg
                           WHERE src.news_external_uid = trg.news_external_uid
                        )
    ;
COMMIT;
END;
$procedure$
;
