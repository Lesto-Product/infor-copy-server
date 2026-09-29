/* =====================================================================
   original_ticst001  (LN: ticst001 -> LN_ticst001)
   ---------------------------------------------------------------------
   Материали по производствена поръчка (estimated materials).
   Полета: pdno, pono, opno, sitm, qune, ques, timestamp
   Ключ:   (pdno, pono) - [pono] е позицията ВЪТРЕ в поръчката и се
           повтаря между поръчките, затова без [pdno] редовете се губят
           при дедупликацията на MERGE-а.

   ВАЖНО: скриптът на sync-а НЕ създава целеви таблици - той създава само
   ##Temp таблици и прави MERGE в [dbo].[<localTable>]. Затова този DDL
   трябва да мине ръчно ПРЕДИ първия sync.

   Типовете следват конвенцията на local.provider.js: съставен ключ ->
   NVARCHAR(50), останалите -> NVARCHAR(MAX). Всичко идва от JDBC като
   string (cloud.provider.js прави String(val)).

   Изпълни в база: Lesto
   ===================================================================== */

USE [Lesto];
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'[dbo].[original_ticst001]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[original_ticst001]
    (
        [pdno]      NVARCHAR(50)  NOT NULL,  -- производствена поръчка
        [pono]      NVARCHAR(50)  NOT NULL,  -- позиция в поръчката
        [opno]      NVARCHAR(MAX) NULL,      -- операция (0 идва като NULL, виж cloud.provider.js)
        [sitm]      NVARCHAR(MAX) NULL,      -- материал
        [qune]      NVARCHAR(MAX) NULL,      -- нетно количество
        [ques]      NVARCHAR(MAX) NULL,      -- планирано количество
        [timestamp] NVARCHAR(MAX) NULL,      -- за инкременталния sync
        CONSTRAINT [PK_original_ticst001] PRIMARY KEY CLUSTERED ([pdno], [pono])
    );

    PRINT 'CREATED: dbo.original_ticst001';
END
ELSE
    PRINT 'SKIP: dbo.original_ticst001 вече съществува';
GO

/* ---------------------------------------------------------------------
   Зареждане. Първият sync намира празна таблица и прави full reload;
   след това върви инкрементално по [timestamp].

       POST http://<host>:3005/trigger/ticst001
   --------------------------------------------------------------------- */

/* ---------------------------------------------------------------------
   Проверка
   --------------------------------------------------------------------- */
-- SELECT COUNT(*) AS total, COUNT(DISTINCT pdno) AS orders
-- FROM [dbo].[original_ticst001];
GO
