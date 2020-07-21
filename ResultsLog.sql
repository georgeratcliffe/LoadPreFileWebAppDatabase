﻿CREATE TABLE [dbo].[ResultsLog](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[DateTime] [datetime2](7) NOT NULL,
	[Message] [nvarchar](200) NULL,
 CONSTRAINT [PK_ResultsLog] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
