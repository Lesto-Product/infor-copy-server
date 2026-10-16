/* =====================================================================
   original_tccom001  (LN: tccom001 -> LN_tccom001)
   ---------------------------------------------------------------------
   Служители (Employees - General): кодът (EMP251) и името на служителя.
   Полета: emno, nama_bg_BG
   Ключ:   emno

   Нужна е на Таблото Производство (страница „Служители“), което дотук
   показваше само кодовете от tbptmm120.emno. Datalake API-то
   (infor-cloud-be, /time-tracking/*) прави LEFT JOIN към тази таблица и
   връща `name`; ако таблицата липсва, API-то тихо връща name = null и
   таблото показва кодовете както преди.

   ВАЖНО: скриптът на sync-а НЕ създава целеви таблици - той създава само
   ##Temp таблици и прави DELETE + INSERT (пълно презареждане, защото
   incrementalColumn е null). Затова този DDL трябва да мине ръчно ПРЕДИ
   първия sync.

   NVARCHAR (не VARCHAR) е задължително - имената са на кирилица.
   Типовете следват конвенцията на local.provider.js: ключ -> NVARCHAR(50),
   име -> NVARCHAR(255) както nama_bg_BG в original_tccom130.

   Изпълни в база: Lesto (192.168.1.187\SQLEXPRESS)
   ===================================================================== */

USE [Lesto];
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF OBJECT_ID(N'[dbo].[original_tccom001]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[original_tccom001]
    (
        [emno]       NVARCHAR(50)  NOT NULL,  -- код на служителя (EMP251)
        [nama_bg_BG] NVARCHAR(255) COLLATE Latin1_General_CI_AS NULL,  -- име
        CONSTRAINT [PK_original_tccom001] PRIMARY KEY CLUSTERED ([emno])
    );

    PRINT 'CREATED: dbo.original_tccom001';
END
ELSE
    PRINT 'SKIP: dbo.original_tccom001 вече съществува';
GO

/* ---------------------------------------------------------------------
   Зареждане. Таблицата е в сутрешния (05:00) и обедния (12:00) batch;
   за първо зареждане веднага след деплоя на новия код:

       POST http://192.168.54.244:3005/trigger/tccom001
   --------------------------------------------------------------------- */

/* ---------------------------------------------------------------------
   Проверка
   --------------------------------------------------------------------- */
-- SELECT COUNT(*) AS total FROM [dbo].[original_tccom001];
-- SELECT TOP 20 emno, nama_bg_BG FROM [dbo].[original_tccom001] ORDER BY emno;
GO
