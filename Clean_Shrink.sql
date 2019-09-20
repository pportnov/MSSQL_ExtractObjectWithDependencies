
--------------------------------------------------------------------------------
--переменные
--------------------------------------------------------------------------------
declare @cmd nvarchar(max)

set nocount on 

--------------------------------------------------------------------------------
--Удалить лишнее
--------------------------------------------------------------------------------
if object_id('GetForeignKeys') is not null  drop function GetForeignKeys
if object_id('GetIdColumnName') is not null drop function dbo.GetIdColumnName

if object_id('GetKeysIdTable') is not null drop procedure GetKeysIdTable
if object_id('GetCommaSeparatedColumn') is not null drop  procedure [dbo].[GetCommaSeparatedColumn]
if object_id('IsTableIdentity') is not null drop procedure IsTableIdentity
if object_id('MegaExport') is not null drop procedure MegaExport

if exists (select 1 from sys.table_types where name = 'ForeignKeysTableType') drop type ForeignKeysTableType
if exists (select 1 from sys.table_types where name = 'KeysIds') drop type KeysIds

if object_id('MegaExportCmds') is not null drop table MegaExportCmds 
if object_id('MegaExportLog') is not null drop table MegaExportLog 
if object_id('Dict') is not null drop table Dict 

--------------------------------------------------------------------------------
--обновить статистики, перестроить кучи, сжать лобы, дефрагментировать индексы
--------------------------------------------------------------------------------

declare cur cursor fast_forward forward_only read_only for 
	select	'update statistics ['+schema_name(o.schema_id)+'].['+o.[name]+'];'+char(13)+
		case when i.[type_desc] = 'HEAP' then 'alter table ['+schema_name(o.schema_id)+'].['+o.[name]+'] rebuild;'
		else 'alter index ['+i.[name]+'] on ['+schema_name(o.schema_id)+'].['+o.[name]+'] reorganize with (lob_compaction = on);'
		end	as cmd
	from sys.indexes i 
	join sys.objects o on o.object_id = i.object_id and o.[type] = 'U'
open cur 
fetch next from cur into @cmd
while @@FETCH_STATUS = 0 begin 
	exec (@cmd)

	fetch next from cur into @cmd
end 
close cur
deallocate cur

--------------------------------------------------------------------------------
--Шринк в конце
--------------------------------------------------------------------------------
declare @DataFileName varchar(255) 
declare @LogFileName varchar(255)

declare @DataTargetSize int 
declare @LogTargetSize int

--у нас только по 1 файлу в файловой группе и в бд
select top 1
	@DataFileName = [name]
	,@DataTargetSize=(FILEPROPERTY([name], 'SpaceUsed') / 128)+50 
FROM sys.database_files where [type] = 0

select top 1
	@LogFileName = [name]
	,@LogTargetSize=(FILEPROPERTY([name], 'SpaceUsed') / 128)+10 
FROM sys.database_files where type != 0


DBCC SHRINKFILE (@DataFIleName , @DataTargetSize) WITH NO_INFOMSGS
DBCC SHRINKFILE (@DataFIleName , @DataTargetSize) WITH NO_INFOMSGS
DBCC SHRINKFILE (@LogFileName , @LogTargetSize) WITH NO_INFOMSGS
DBCC SHRINKFILE (@LogFileName , @LogTargetSize) WITH NO_INFOMSGS
DBCC SHRINKFILE (@LogFileName , @LogTargetSize) WITH NO_INFOMSGS

