/* ---------------------------------------------------------------------
   Добавя [nama_bg_BG] (ИМЕ НА ФИРМАТА) към original_tccom130.

   ВНИМАНИЕ - nama НЕ Е namc. Различават се с една буква:
     nama_bg_BG -> 'GABRIEL TRANSPORT AG'   <- това добавяме
     namc_bg_BG -> 'Herdern No.17'          <- улицата, вече се извлича
     dsca_bg_BG -> 'Nidwalden'              <- областта (от tcmcs143), вече се извлича

   NVARCHAR (не VARCHAR) е задължително - българските имена иначе стават
   '??????'. Целият път на зареждането е Unicode: temp таблицата в
   local.provider.js е NVARCHAR, bulk-ът подава sql.NVarChar, а MERGE-ът
   не използва литерали.

   ВАЖНО - изпълни ТОВА ПРЕДИ да качиш новия код на сървъра.
   MERGE-ът вмъква изрично колона [nama_bg_BG]; ако я няма в целевата
   таблица, целият sync на tccom130 пада (включително нощният в 05:00).
   --------------------------------------------------------------------- */
USE [Lesto];
GO

IF COL_LENGTH('dbo.original_tccom130', 'nama_bg_BG') IS NULL
BEGIN
    ALTER TABLE [dbo].[original_tccom130]
        ADD [nama_bg_BG] NVARCHAR(255) COLLATE Latin1_General_CI_AS NULL;
    PRINT 'ADDED: dbo.original_tccom130.nama_bg_BG';
END
ELSE
    PRINT 'SKIP: dbo.original_tccom130.nama_bg_BG вече съществува';
GO

/* ---------------------------------------------------------------------
   Зареждане. tccom130 е с incrementalColumn: null и без baseFilter, т.е.
   всеки sync тегли всичките 1252 реда и MERGE-ът ги update-ва по [cadr].
   Значи НЕ е нужен TRUNCATE - първият sync след деплоя попълва колоната
   за всички съществуващи редове.

       POST http://<host>:3005/trigger/tccom130
   --------------------------------------------------------------------- */

/* ---------------------------------------------------------------------
   Проверка
   --------------------------------------------------------------------- */
-- SELECT cadr, nama_bg_BG, namc_bg_BG
-- FROM [dbo].[original_tccom130]
-- WHERE cadr IN ('ADR000287','ADR000610');
-- -- ADR000287 | KOMAX AG             | Industriestrasse str., 6
-- -- ADR000610 | GABRIEL TRANSPORT AG | Herdern No.17

-- SELECT COUNT(*) AS total, COUNT(nama_bg_BG) AS with_name
-- FROM [dbo].[original_tccom130];   -- total трябва да остане 1252
GO
