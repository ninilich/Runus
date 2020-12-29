CREATE OR REPLACE PROCEDURE public.fill_news_x_entity()
 LANGUAGE plpgsql
AS $procedure$

BEGIN
    WITH
    stg AS -- выборка данных их стейджа
    (
      SELECT new_external_uid,
             entity_name,
             entity_type_id
      FROM (
            SELECT news_uid AS new_external_uid,
                   upper(entity_nm) AS entity_name,
                   entity_type AS entity_type_id,
                   row_number() OVER (PARTITION BY news_uid, entity_nm, entity_type ORDER BY news_datetime desc) AS rn
            FROM public.stg_news
          ) t
      WHERE rn = 1 -- для устранения дублей в стейдже
    ),
    src AS
    (
        SELECT DISTINCT n.news_id , e.entity_id
          FROM stg
          JOIN public.news n ON n.news_external_uid = stg.new_external_uid
          LEFT JOIN public.ref_synonyms s ON upper(s.synonim_entity_nm ) = stg.entity_name
          JOIN public.entity e ON e.entity_name = COALESCE (upper(s.entity_nm), stg.entity_name) AND
                                  e.entity_type_id  = stg.entity_type_id
          WHERE NOT EXISTS ( -- не исключение
                            SELECT 1 FROM public.ref_exeptions ex
                              WHERE upper(ex.entity_name) = stg.entity_name AND
                                    ex.entity_type_id = stg.entity_type_id
                            )
    )
    INSERT INTO public.news_x_entity (news_id, entity_id)
    SELECT news_id, entity_id
      FROM src
      WHERE NOT EXISTS (
                         SELECT 1 FROM public.news_x_entity trg
                           WHERE src.news_id = trg.news_id AND
                                 src.entity_id = trg.entity_id
                        );
    COMMIT;
END;
$procedure$
;
