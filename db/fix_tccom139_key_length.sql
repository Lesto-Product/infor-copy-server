/* ---------------------------------------------------------------------
   Смаляване на ключовите колони на original_tccom139.
   NVARCHAR(200) x3 = 1200B > 900B (лимитът за CLUSTERED индекс) -> warning.
   Реалните стойности са <= 8 знака (ccty='BG', cste='PL', city='00000018'),
   така че NVARCHAR(50) дава предостатъчно запас: 3 x 100B = 300B.
   Безопасно е само докато таблицата е празна.
   --------------------------------------------------------------------- */
USE [Lesto];
GO

IF EXISTS (SELECT 1 FROM [dbo].[original_tccom139])
BEGIN
    RAISERROR('original_tccom139 не е празна - спри и провери преди ALTER.', 16, 1);
    RETURN;
END
GO

ALTER TABLE [dbo].[original_tccom139] DROP CONSTRAINT [PK_original_tccom139];
GO

ALTER TABLE [dbo].[original_tccom139] ALTER COLUMN [ccty] NVARCHAR(50) NOT NULL;
ALTER TABLE [dbo].[original_tccom139] ALTER COLUMN [cste] NVARCHAR(50) NOT NULL;
ALTER TABLE [dbo].[original_tccom139] ALTER COLUMN [city] NVARCHAR(50) NOT NULL;
GO

ALTER TABLE [dbo].[original_tccom139]
    ADD CONSTRAINT [PK_original_tccom139] PRIMARY KEY CLUSTERED ([ccty], [cste], [city]);
GO

PRINT 'OK: PK_original_tccom139 пресъздаден - 300B ключ, без warning.';
GO
