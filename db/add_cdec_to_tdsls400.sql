/* ---------------------------------------------------------------------
   Добавя [cdec] (delivery terms на ниво поръчка) към original_tdsls400.
   1 674 от 4 839 поръчки имат cdec, различен от този на BP-то, така че
   order-level стойността е тази, която се печата.

   ВАЖНО - изпълни ТОВА ПРЕДИ да пуснеш sync на tdsls400 с новия код.
   MERGE-ът вмъква изрично колона [cdec]; ако я няма в целевата таблица,
   цялата синхронизация на tdsls400 пада (включително нощната в 05:00).
   --------------------------------------------------------------------- */
USE [Lesto];
GO

IF COL_LENGTH('dbo.original_tdsls400', 'cdec') IS NULL
BEGIN
    ALTER TABLE [dbo].[original_tdsls400] ADD [cdec] NVARCHAR(MAX) NULL;
    PRINT 'ADDED: dbo.original_tdsls400.cdec';
END
ELSE
    PRINT 'SKIP: dbo.original_tdsls400.cdec вече съществува';
GO

/* ---------------------------------------------------------------------
   Backfill. tdsls400 е инкрементална по [timestamp] - старите (приключени)
   поръчки няма да се докоснат никога, значи cdec ще им остане NULL.
   Изпразването на таблицата кара sync.service.js да мине по bootstrap
   пътя (countRows() === 0 -> fullReload) и презарежда всичко наведнъж.

   Обхватът не се променя: extract-ът винаги е бил с baseFilter
   ddat > '2020-01-01', така че нищо извън него не се губи.

   Пусни го САМО заедно с ръчния trigger веднага след това:
       POST http://<host>:3005/trigger/tdsls400
   --------------------------------------------------------------------- */
-- SELECT COUNT(*) AS rows_before FROM [dbo].[original_tdsls400];
-- TRUNCATE TABLE [dbo].[original_tdsls400];
GO
