/* =====================================================================
   client-info: локални таблици за GET /sales/client-info
   ---------------------------------------------------------------------
   Създава двете липсващи огледални таблици:
     original_tccom110  - SUPPLIER No (osno) + delivery terms по подразбиране
     original_tccom139  - master на градовете, ключ (ccty, cste, city)

   ВАЖНО: скриптът на sync-а НЕ създава целеви таблици - той създава само
   ##Temp таблици и прави MERGE в [dbo].[<localTable>]. Затова тези DDL-и
   трябва да минат ръчно ПРЕДИ първия sync.

   Типовете следват конвенцията на local.provider.js:
     - ключовите колони -> NVARCHAR(200), а при съставен ключ -> NVARCHAR(50),
       защото CLUSTERED индексът приема максимум 900B ключ
     - останалите       -> NVARCHAR(MAX)
   Всичко идва от JDBC като string (cloud.provider.js прави String(val)),
   затова и датите се пазят като текст - без implicit convert при MERGE.

   Изпълни в база: Lesto
   ===================================================================== */

USE [Lesto];
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* ---------------------------------------------------------------------
   1) original_tccom110  (LN: tccom110 -> LN_tccom110)
      Полета: ofbp, cdec, osno, cadr, stdt, endt
      Ключ:   ofbp  (една отворена редица на BP; endt = 1970-01-01 е
              нулевата дата на LN, не се филтрира по ефективност)
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'[dbo].[original_tccom110]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[original_tccom110]
    (
        [ofbp] NVARCHAR(200) NOT NULL,   -- BP id (в tccom100 се казва bpid)
        [cdec] NVARCHAR(MAX) NULL,       -- delivery terms по подразбиране за BP
        [osno] NVARCHAR(MAX) NULL,       -- SUPPLIER No (нашият номер при клиента)
        [cadr] NVARCHAR(MAX) NULL,       -- адрес по подразбиране -> tccom130.cadr
        [stdt] NVARCHAR(50)  NULL,       -- начало на валидност (текст от JDBC)
        [endt] NVARCHAR(50)  NULL,       -- край на валидност (1970-01-01 = отворена)
        CONSTRAINT [PK_original_tccom110] PRIMARY KEY CLUSTERED ([ofbp])
    );

    PRINT 'CREATED: dbo.original_tccom110';
END
ELSE
    PRINT 'SKIP: dbo.original_tccom110 вече съществува';
GO

/* ---------------------------------------------------------------------
   2) original_tccom139  (LN: tccom139 -> LN_tccom139)
      Полета: ccty, cste, city, dsca_bg_BG   (~250 реда)
      Ключ:   (ccty, cste, city) - СЪСТАВЕН, никога само кодът на града!
              BG/PL/00000018 = Плевен, BG/VR/00000018 = гр. Враца,
              BG/../00000176 = Добрич, IT/../00000176 = Tezze sul Brenta.
      Кодовете са zero-padded 8-символни низове ('00000018'), не '18'.
      NB: колоната се казва [city] тук, но [ccit] в tccom130.
   --------------------------------------------------------------------- */
IF OBJECT_ID(N'[dbo].[original_tccom139]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[original_tccom139]
    (
        [ccty]       NVARCHAR(50)  NOT NULL,  -- държава
        [cste]       NVARCHAR(50)  NOT NULL,  -- област / щат
        [city]       NVARCHAR(50)  NOT NULL,  -- код на града (8 знака, zero-padded)
        [dsca_bg_BG] NVARCHAR(MAX) NULL,      -- име на града
        CONSTRAINT [PK_original_tccom139] PRIMARY KEY CLUSTERED ([ccty], [cste], [city])
    );

    PRINT 'CREATED: dbo.original_tccom139';
END
ELSE
    PRINT 'SKIP: dbo.original_tccom139 вече съществува';
GO

/* ---------------------------------------------------------------------
   Проверка
   --------------------------------------------------------------------- */
SELECT  t.name                                   AS [table],
        c.name                                   AS [column],
        ty.name + CASE WHEN ty.name LIKE 'nvarchar%'
                       THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX'
                                       ELSE CAST(c.max_length / 2 AS VARCHAR(10)) END + ')'
                       ELSE '' END               AS [type],
        c.is_nullable                            AS [nullable],
        c.column_id                              AS [ord]
FROM sys.tables  t
JOIN sys.columns c  ON c.object_id = t.object_id
JOIN sys.types   ty ON ty.user_type_id = c.user_type_id
WHERE t.name IN (N'original_tccom110', N'original_tccom139')
ORDER BY t.name, c.column_id;
GO
