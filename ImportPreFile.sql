CREATE PROCEDURE [dbo].[ImportPreFile] (@filePath VARCHAR(200))
AS
BEGIN

BEGIN TRY

insert into dbo.ResultsLog
 select getdate(), 'Load started for ' + @filePath

if object_id('tempdb..#input') is not null drop table #input
create table #input ( Line varchar(MAX) not null)

if object_id('tempdb..#lines') is not null drop table #lines
create table #lines(id int IDENTITY(1,1) PRIMARY KEY, val varchar(100))

if object_id('tempdb..#linesparsed') is not null drop table #linesparsed
create table #linesparsed(id int,val varchar(100))

declare @starttime datetime = getdate()

declare @SQL nvarchar(200) = ''

set @sql = N'
 BULK INSERT #input
 FROM ''' + @filePath + ''''

exec sp_executesql @SQL


update #input 
set Line =  replace(Line, 'Grid-ref', ' Grid-ref')
	
declare @line varchar(MAX)
select @line = line from #input

insert into #lines
select value from string_split(@line, ' ')

   
-- Remove invalid rainfall values

insert into #linesparsed
  select * from #lines
   where isnumeric( dbo.udf_GetNumeric(val)) = 1
   and len(val) > 8
   and id > 50


declare @id int
declare @idmax int
declare @idmin int
declare @minxref varchar(20)
declare @minyref varchar(20)
declare @badval varchar(20)

DECLARE curpar CURSOR
FOR SELECT 
        id FROM 
        #linesparsed;

OPEN curpar;

FETCH NEXT FROM curpar INTO 
    @id;

WHILE @@FETCH_STATUS = 0
    BEGIN

	select @idmin = max(id) from #lines
    where val like '%grid-ref%'
	and id < @id

	select @idmax = min(id) from #lines
    where val like '%grid-ref%'
	and id > @id

	select @badval = val from #lines
	 where id = @id

	select @minxref = val from #lines
	 where id = @idmin + 2

	select @minyref = val from #lines
	 where id = @idmin + 3

	insert into dbo.ResultsLog
     select getdate(), 'Invalid Rainfall ' + @badval + ' Grid-ref data not loaded X:' + @minxref + ' Y:' + @minyref

	delete from #lines
	 where id between @idmin and @idmax - 1

		FETCH NEXT FROM curpar INTO 
            @id
    END;

CLOSE curpar;

DEALLOCATE curpar;


delete from #lines
where val = ''

-- Begin inserting into dbo.Results

truncate table dbo.Results

declare @val varchar(100)
declare @FromYear int
declare @ToYear int
declare @Xref int = -1
declare @Yref int = -1
declare @yearcnt int = 0


DECLARE cur CURSOR
FOR SELECT 
        val FROM 
        #lines;

OPEN cur;

FETCH NEXT FROM cur INTO 
    @val;

WHILE @@FETCH_STATUS = 0
    BEGIN
			IF (charindex('Years', @val) > 0)
			BEGIN
				select
					@FromYear = substring(@val, charindex('Years', @val) + 6, 4),
					@ToYear = substring(@val, charindex('Years', @val) + 11, 4)
			END

			ELSE IF (charindex('Grid-ref', @val) > 0)
			BEGIN

				FETCH NEXT FROM cur INTO  @val
				select @Xref = cast( substring(@val, charindex(',', trim(@val)) -4, 4) as int)
				FETCH NEXT FROM cur INTO  @val
				set @val = dbo.udf_GetNumeric(@val)
				select @Yref = cast( @val as int)

				set @yearcnt = 0
			END

			ELSE IF (@Xref > -1 AND @Yref > -1)
			BEGIN

				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 1,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 2,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 3,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 4,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 5,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 6,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 7,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 8,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 9,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 10,1), @val

				FETCH NEXT FROM cur INTO  @val
				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 11,1), @val
				  
				FETCH NEXT FROM cur INTO  @val
				 set @val = dbo.udf_GetNumeric(@val)

				insert into dbo.Results
				 select @Xref, @Yref, DATEFROMPARTS( @FromYear + @yearcnt, 12,1), @val

				set @yearcnt = @yearcnt + 1

			END

	        FETCH NEXT FROM cur INTO 
            @val
    END;

CLOSE cur;

DEALLOCATE cur;

declare @rowcount int
select @rowcount = count(*) from dbo.Results

insert into dbo.ResultsLog
select getdate(), 'File Load Completed, Number of rows in dbo.Results :' +  cast(@rowcount as varchar(10))


select * from ResultsLog
where DateTime >= @starttime

END TRY
BEGIN CATCH

insert into dbo.ResultsLog
select getdate(), 'ERROR - Process terminated: ' + ERROR_MESSAGE()

select * from ResultsLog
where DateTime >= @starttime

IF CURSOR_STATUS('global','cur')>=-1
BEGIN
 DEALLOCATE cur
END

IF CURSOR_STATUS('global','curpar')>=-1
BEGIN
 DEALLOCATE curpar
END

END CATCH

END
GO

