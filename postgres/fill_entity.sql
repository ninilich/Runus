CREATE OR REPLACE PROCEDURE public.fill_entity()
 LANGUAGE plpgsql
AS $procedure$

BEGIN

WITH src AS
(
    SELECT DISTINCT
           upper(entity_nm) AS entity_name,
           entity_type AS entity_type_id
      FROM public.stg_news n
      WHERE length(entity_nm) <= 50 -- отбрасываем явные ошибки
        -- не в списке исключений
        AND NOT EXISTS ( SELECT 1 FROM public.ref_exeptions e
                           WHERE e.entity_type_id = n.entity_type AND
                                 upper(e.entity_name) = upper(n.entity_nm )
                       )

        -- только имеющиеся типы
        AND entity_type IN (SELECT entity_type_id FROM public.ref_entity_type)
        -- не синонимы
        AND upper(entity_nm) NOT IN (SELECT upper(synonim_entity_nm) FROM public.ref_synonyms)
)
INSERT INTO public.entity (entity_name, entity_type_id)
SELECT entity_name, entity_type_id
  FROM src
  WHERE NOT EXISTS (
                     SELECT 1 FROM public.entity trg
                       WHERE upper(src.entity_name) = upper(trg.entity_name) AND
                             src.entity_type_id = trg.entity_type_id
                    );
COMMIT;
END;
$procedure$
;
