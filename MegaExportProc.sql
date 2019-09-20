--Закопировать все по связям 
--скрипт выполнять в контексте бд @dbSource, определения ниже для генерации скриптов, скрипт не переключает контекст!

/*
--Определить начало
set nocount on
declare @t KeysIds
insert into @t values (2),(8),(1966926),(1965197),(196692),(2910862),(4052689)
exec MegaExport 
	@dbSource = 'YouDo'
	,@dbTarget = 'YouDo_Preseed'
	,@Table = 'Users'
	,@KeysColumn = 'id'	
	,@KeysId =@t	
	,@TopRows = 1024
	,@DebugMsgsEnble=1
*/

/* Удалить все скрипты
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
*/
------------------------------------------------------------------------------------------------------------------------
if object_id('MegaExportCmds') is not null drop table MegaExportCmds 
Create table MegaExportCmds (id int identity(1,1), lvl int default (-1), dbg bit default(0), cmd  varchar(max) null)

if object_id('MegaExportLog') is not null drop table MegaExportLog 
--конвертация занимает слишком много времнени 
Create table MegaExportLog (StepId uniqueidentifier, tbl varchar(255), Constr  varchar(255) null, KeyId varchar(128)
--Create table MegaExportLog (StepId uniqueidentifier, tbl varchar(255), Constr  varchar(255) null, KeyId bigint 
, index cix clustered (tbl,Constr,KeyId) , index ix (StepId))
Go
------------------------------------------------------------------------------------------------------------------------
create or alter FUNCTION [dbo].[GetForeignKeys](@Table Nvarchar(255), @Schema Nvarchar(255) = null) 
RETURNS TABLE
as return (
	select object_name(f.object_id) ConstraintName
	,object_name(fc.parent_object_id) TableF, cf.name columnF
	,object_name(fc.referenced_object_id)  TableT, ct.name columnT
	from sys.foreign_key_columns fc 
	join sys.columns cf on cf.object_id=fc.parent_object_id and cf.column_id = fc.parent_column_id
	join sys.columns ct on ct.object_id=fc.referenced_object_id and ct.column_id = fc.referenced_column_id
	join sys.foreign_keys f on fc.constraint_object_id = f.object_id 
	where (
		fc.parent_object_id = object_id(@Table)	or fc.referenced_object_id = object_id(@Table)
	)
	and f.is_disabled = 0 
	and f.type = 'F'
	and f.schema_id = case when SCHEMA_ID(@Schema) is null then f.schema_id else SCHEMA_ID(@Schema) end
)
Go
------------------------------------------------------------------------------------------------------------------------
if exists (select 1 from sys.table_types where name = 'ForeignKeysTableType')	
	drop type ForeignKeysTableType
CREATE TYPE ForeignKeysTableType AS TABLE 
(
	ConstraintName nvarchar(255) not null 
	,TableF nvarchar(255) not null 
	,columnF nvarchar(255) not null 
	,TableT nvarchar(255) not null 
	,columnT nvarchar(255) not null 
)
GO
------------------------------------------------------------------------------------------------------------------------
if object_id('GetKeysIdTable') is not null drop procedure GetKeysIdTable
if object_id('MegaExport') is not null drop procedure MegaExport
if exists (select 1 from sys.table_types where name = 'KeysIds') drop type KeysIds
-- id может быть и int и bigint и GUID (Guests) и Date (Dates) и даже varchar (PromoCodesKeys) и nvarchar(TaskTemplate)
CREATE TYPE KeysIds AS TABLE (Id sql_variant not null)  -- конвертация занимает много времени, если id строго цифры, лучше использовать bigint
--CREATE TYPE KeysIds AS TABLE (Id bigint not null)
GO
------------------------------------------------------------------------------------------------------------------------
create or alter function dbo.GetIdColumnName(@tbl nvarchar(255)) 
returns nvarchar(255) as begin 
	return (
		select c.name
		from sys.indexes i 
		join sys.index_columns ic on i.index_id = ic.index_id and i.object_id = ic.object_id
		join sys.columns c on ic.column_id = c.column_id and c.object_id = ic.object_id
		where i.object_id = object_id(@tbl) and i.is_primary_key=1
		and i.object_id in (
			-- не обрабатываем составные ключи (вернется null)
			select i.object_id
			from sys.indexes i 
			join sys.index_columns ic on i.index_id = ic.index_id and i.object_id = ic.object_id
			where i.is_primary_key=1
			group by i.object_id
			having count(ic.column_id) = 1
		)
	)	
end
GO
------------------------------------------------------------------------------------------------------------------------
create or alter procedure [dbo].[GetKeysIdTable](
	 @DbName			varchar(255)
	,@TableName			varchar(255)
	,@ValueColumnName	varchar(255)
	,@FilterColumnName  varchar(255)	
	,@FilterValues		KeysIds readonly
	,@TopRows			int = 1024
	,@lvl				int = -1	
	,@DebugMsgsEnble	bit = 0
) 
as begin 
	
	--всетаки еще раз проверим кол-во строк
	declare @rcnt int = (select count(1) from @FilterValues)
	if @rcnt > @TopRows set @TopRows = @rcnt 

	declare @cmd nvarchar(max)
	declare @parameters nvarchar(max)

	set @cmd = N'		
		select top ('+trim(cast(@TopRows as char))+') t.['+@ValueColumnName+']		
		from '+@DbName+'..'+@TableName + ' as t
		join @FilterValues f on t.['+@FilterColumnName+']= f.id 
		where t.['+@ValueColumnName+'] is not null		
		order by t.['+@FilterColumnName+']'

	set @parameters = N'@FilterValues KeysIds readonly'	
		 
	--debug info 
	if @DebugMsgsEnble = 1 begin 
		
		insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'GetKeysIdTable @TableName-'+@TableName+' @ValueColumnName-'+@ValueColumnName+' @FilterColumnName-'+@FilterColumnName+' KeysIds(Cnt)-'+cast(@rcnt as char))
		insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'GetKeysIdTable @cmd-'+@cmd)

		--declare @KeysIdArray varchar(max)
		--set @KeysIdArray  = (select stuff((select distinct ','+cast(id as varchar) from @FilterValues for xml path('')),1,1,''))
		--insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'GetKeysIdTable @KeysIdArray-'+@KeysIdArray)
	end 

	begin try 	
		exec sp_executesql @cmd, @parameters, @FilterValues
	end try		
	begin catch
		insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'GetKeysIdTable.ERROR proc-'+ERROR_PROCEDURE())
		insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'GetKeysIdTable.ERROR line-'+cast(ERROR_LINE() as char))
		insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'GetKeysIdTable.ERROR msg-'+ERROR_MESSAGE() )
	end catch 	
end
go
------------------------------------------------------------------------------------------------------------------------
create or alter procedure GetIdentityColumn (@db varchar(255), @table varchar(255), @IdentityColumn varchar(255) output)
as begin 	
	declare @x table (x varchar(255))
	declare @cmd varchar(max)
	set @cmd =  'select [name] from '+@db+'.sys.columns where object_id = object_id('''+@db+'..'+@Table+''') and is_identity=1'
	insert into @x exec(@cmd) 
	
	set @IdentityColumn = (select x from @x)
end 
go
------------------------------------------------------------------------------------------------------------------------
create or alter procedure MegaExport 
(
	@dbSource varchar(255)
	,@dbTarget varchar(255)
	,@Table varchar(255)
	,@KeysColumn varchar(255)	
	,@KeysId KeysIds readonly
	,@constr varchar(255) = null
	,@lvl smallint=0
	,@DebugMsgsEnble bit = 0
	,@NoRecursion bit = 0
	,@TopRows  int = 1024
	,@execute bit = 0 
)
as begin

	declare @Stepid uniqueidentifier = NewId()

	--для рекурсии проверка что эта таблица с этими данными не обрабатывалась ранее
	if (select count(1) from @KeysId) > 0  and 
		exists(
			select id 
			from @KeysId  
			except (select KeyId from MegaExportLog l where tbl = @Table)
		)	
	 begin 
	 
		--пишим что обрабатываем в лог, чтобы не обрабатывть повторно		
		merge into MegaExportLog t using (select @Table tbl, @constr constr, id from @KeysId) s 
			on t.KeyId = s.id and isnull(t.Constr,'')= isnull(s.constr,'') and t.tbl=s.tbl		
		when not matched by target then insert (Stepid,tbl, Constr,KeyId) values (@stepid,@Table, @constr, cast(s.id as varchar(128)));
				
		if @DebugMsgsEnble = 1 
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'StartMegEport @table-'+isnull(@Table,'null')+' @constr-'+isnull(@constr,'null'))

		declare @t ForeignKeysTableType

		--получить ключи таблицы
		insert into @t select * from GetForeignKeys(@Table,'dbo')
				
		if @DebugMsgsEnble = 1 begin 
			declare @x int = (select count(1) from @t)
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'StartMegEport @t ForeignKeysCnt-'+cast(@x as char))
		end 
		
		declare @colF nvarchar(255)
		declare @colT nvarchar(255)
		declare @tbl nvarchar(255)
		declare @constri nvarchar(255)
		declare @IdentityColumn varchar(255)
		declare @InsideKeysId KeysIds
		declare @newKeysId KeysIds 
		declare @TmpTableKeys KeysIds
		declare @NextTableKeys KeysIds
		declare @cmd nvarchar(max)
		declare @lvl1 smallint
		--declare @ReseedVar varchar(255)
		declare @TopRowsi int = @TopRows		
		
		declare @rcnt int = (select count(1) from @KeysId)
		if @rcnt > @TopRowsi set @TopRowsi = @rcnt 

		insert into @InsideKeysId(id) select id from  @KeysId -- т.к. @KeysId => readonly
		
		--self keys если таблица ссылается на саму себя, добавляем к списку id по ссылке
		if exists (select 1 from @t where TableF = TableT and TableF = @Table) begin 	
	
			if @DebugMsgsEnble = 1
				insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'SelfConstraint start '+@Table)

			set @constri = null

			--цикл по всем констрейнам где  таблиа ссылается на саму себя 
			while 1=1 begin
				--первое значение цикла 
				if @constri is null begin
					set @constri = (
						select min(ConstraintName) from @t
						where TableF = TableT and TableF = @Table
					)
				end
				--последущие значения цикла
				else begin
					set  @constri = (
						select top 1 ConstraintName from @t 
						where TableF = TableT and TableF = @Table
						and ConstraintName > @constri
						order by ConstraintName 
					)				
				end
				--условие выхода
				if @constri is null break
			
				if @DebugMsgsEnble = 1
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'SelfConstraint @table-'+isnull(@Table,'null')+' @constri-'+isnull(@constri,'null'))
				
				--определить переменые цикла
				select @colF=columnF, @colT=columnT 
				from @t 
				where ConstraintName=@constri
			
				--добавляем к списку новые id по ссылке
				delete from @newKeysId 
				delete from @TmpTableKeys

				insert into @TmpTableKeys 
				exec GetKeysIdTable
					 @DbName			= @dbSource
					,@TableName			= @Table 
					,@ValueColumnName	= @colF
					,@FilterColumnName	= @colT
					,@FilterValues		= @InsideKeysId
					,@lvl				= @lvl					
					,@TopRows			= @TopRowsi
					,@DebugMsgsEnble	= @DebugMsgsEnble				
				insert into @newKeysId select distinct * from @TmpTableKeys

				merge into @InsideKeysId t using @newKeysId s on t.id = s.id
				when not matched by target then insert (id) values (s.id);

				set @TopRowsi += (select count(1) from @newKeysId)

				merge into MegaExportLog t using (select @Table tbl, @constr constr, id from @newKeysId) s 
					on t.KeyId = s.id and isnull(t.Constr,'')= isnull(s.constr,'') and t.tbl=s.tbl		
				when not matched by target then insert (Stepid,tbl, Constr,KeyId) values (@stepid,@Table, @constr, cast(s.id as varchar(128)));
			end 

			if @DebugMsgsEnble = 1
				insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'SelfConstraint end '+@Table )
		end		
	
		--up tables -- пройтись по таблицам на которые ссылается объект	
		if exists (select 1 from @t where TableF = @Table and TableT <> @Table) begin 
			
			if @DebugMsgsEnble = 1
				insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'UpTable start '+@Table)
			
			set @constri = null

			-- цикл по таблицам на которые ссылается объект	
			while 1=1 begin
				--первое значение цикла 
				if @constri is null begin
					set @constri = (
						select min(ConstraintName) from @t
						where TableF = @Table and TableT <> @Table						
						and ConstraintName <> isnull(@constr,'')
					)
				end
				--последущие значения цикла
				else begin
					set  @constri = (
						select top 1 ConstraintName from @t
						where TableF = @Table and TableT <> @Table
						and ConstraintName > @constri
						and ConstraintName <> isnull(@constr,'')
						order by ConstraintName
					)
				end
				--условие выхода
				if @constri is null break
				
				--определить переменые цикла
				select @colF=columnF, @colT=columnT, @tbl = TableT
				from @t
				where ConstraintName=@constri

				if @DebugMsgsEnble = 1
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'UpTable @tbl- '+isnull(@tbl,'null')+' @constri-'+isnull(@constri,'null'))
				
				begin try
					--найти ключи для следующей таблицы
					delete from @NextTableKeys 
					delete from @TmpTableKeys

					insert into @TmpTableKeys 
					exec GetKeysIdTable
						 @DbName			= @dbSource
						,@TableName			= @Table 
						,@ValueColumnName	= @colF
						,@FilterColumnName	= @KeysColumn  
						,@FilterValues		= @InsideKeysId
						,@lvl				= @lvl						
						,@TopRows			= @TopRowsi
						,@DebugMsgsEnble	= @DebugMsgsEnble
					insert into @NextTableKeys select distinct * from @TmpTableKeys

					if @DebugMsgsEnble = 1
						insert into MegaExportCmds(lvl,dbg,cmd) select @lvl,1,'UpTable @NextTableKeysCount '+cast(count(1) as char) from @NextTableKeys 
									
					--рекурсивный вызов следующей таблицы 				
					set @lvl1 = @lvl+1
					exec MegaExport 
						 @dbSource = @dbSource
						,@dbTarget = @dbTarget
						,@Table = @tbl
						,@KeysColumn = @colt
						,@KeysId = @NextTableKeys
						,@constr = @constri 
						,@lvl = @lvl1
						,@DebugMsgsEnble=@DebugMsgsEnble
						,@NoRecursion = 1 --не идем глубже вниз по связям
						,@TopRows = @TopRowsi
						,@execute = @execute
				end try		
				begin catch
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'UpTable.ERROR proc-'+ERROR_PROCEDURE())
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'UpTable.ERROR line-'+cast(ERROR_LINE() as char))
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'UpTable.ERROR msg-'+ERROR_MESSAGE() )
				end catch 
			end 
			if @DebugMsgsEnble = 1
				insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'UpTable end '+@Table)			
		end

		--copy data скопировать собственно данные таблицы
		if @DebugMsgsEnble = 1
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'Copy start '+@Table)
		
		begin try
			--получаем столбцы таблицы
			declare @columns table (txt nvarchar(max))
			declare @ColumnsTxt varchar(1024)
			set @cmd = ('select stuff((
				select '',[''+[name]+'']''
				from '+@dbTarget+'.sys.columns 	
				where object_id = object_id('''+@Table+''')
				and is_computed = 0
				and system_type_id <> 189 --timestamp
				for XML path('''')
				),1,1,'''')')
			insert into @columns exec(@cmd)
			select @ColumnsTxt=txt from @columns	

			--команда копирования данных
			set @cmd = 'insert into  '+@dbTarget+'..'+@Table+' ('+@ColumnsTxt+')'
			set @cmd += char(13) + 'select top ('+trim(cast(@TopRowsi as char))+') '+@ColumnsTxt
			set @cmd += char(13) + 'from '+@dbSource+'..'+@Table+' as t'
			set @cmd += char(13) + 'join (select distinct keyid from MegaExportLog where StepId ='''+cast(@StepId as varchar(36))+''') as l'		
			set @cmd += char(13) + '	on t.['+@KeysColumn+']=l.[KeyId]'
			set @cmd += char(13) + 'where t.['+@KeysColumn+'] not in (select ['+@KeysColumn+'] from '+@dbTarget+'..'+@Table+')'			
			set @cmd += char(13) + 'order by t.['+@KeysColumn+']'  		
		
			--если таблица с idntity, добавить команды отключения/включения идентити
			set @IdentityColumn =null
			exec GetIdentityColumn @dbTarget, @Table, @IdentityColumn output 

			if @IdentityColumn is not null begin 		
				--set @ReseedVar = replace(newid(),'-','')
				set @cmd =  'set identity_insert '+@dbTarget+'..'+@Table+' on' +char(13)+ @cmd+char(13)
				set @cmd += 'set identity_insert '+@dbTarget+'..'+@Table+' off'+char(13)
			end 

			--check self Если таблица имеет ссылки на саму себя, такой констрейнт нужно удалить, и пересоздать после 
			if exists (select 1 from @t where TableF = @Table and TableT = @Table) begin 	

				/* вообще тут должен быть цикл, не доделал
				set @constri = null
				-- цикл по таблицам ссылающимися на себя
				while 1=1 begin
					--первое значение цикла 
					if @constri is null begin
						set @constri = (
							select min(ConstraintName) from @t
							where  TableF = @Table and TableT = @Table							
						)
					end
					--последущие значения цикла
					else begin
						set  @constri = (
							select top 1 ConstraintName from @t
							where TableF = @Table and TableT <> @Table
							and ConstraintName > @constri							
							order by ConstraintName
						)
					end
					--условие выхода
					if @constri is null break

					set @cmd = 'alter table '+@dbTarget+'..'+@Table+' drop constraint '+@constri +char(13)+ @cmd	
					set @cmd += char(13) + 'alter table '+@dbTarget+'..'+@Table 
					set @cmd +=	char(13) + 'with nocheck add constraint '+@constri 
					set @cmd +=	char(13) + 'foreign key (['+@colF+'])'
					set @cmd +=	char(13) + 'references '+@dbTarget+'..'+@Table+' (['+@colT+'])'
					set @cmd +=	char(13) + 'alter table '+@dbTarget+'..'+@Table+' check constraint '+  @constri	
				end */
				
				--скрипты удаления и создания констрейнта
				select @colF=columnF, @colT=columnT, @constri=ConstraintName from @t
				where TableF = TableT and TableT = @Table	

				set @cmd = 'alter table '+@dbTarget+'..'+@Table+' drop constraint '+@constri +char(13)+ @cmd	
				set @cmd += char(13) + 'alter table '+@dbTarget+'..'+@Table 
				set @cmd +=	char(13) + 'with nocheck add constraint '+@constri 
				set @cmd +=	char(13) + 'foreign key (['+@colF+'])'
				set @cmd +=	char(13) + 'references '+@dbTarget+'..'+@Table+' (['+@colT+'])'
				set @cmd +=	char(13) + 'alter table '+@dbTarget+'..'+@Table+' check constraint '+  @constri	
			end

			--set @cmd += char(13)+N'GO'+char(13)
			set @cmd += char(13)

			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,0,@cmd)
			if @execute = 1 begin 
				exec (@cmd)
				--delete from MegaExportLog where StepId = @Stepid  нельзя мы же проверяем MegaExportLog на повторную обработку
			end 

		end try		
		begin catch
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl, 1,'Copy.ERROR proc-'+ERROR_PROCEDURE())
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl, 1,'Copy.ERROR line-'+cast(ERROR_LINE() as char))
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl, 1,'Copy.ERROR msg-'+ERROR_MESSAGE())
		end catch 
	
		if @DebugMsgsEnble = 1
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'Copy end '+@Table)
	
		--down tables пройтись по таблицам которые ссылаются на объект
		if exists (
			select 1 from @t where TableT = @Table and TableF <> @Table
		)  and @NoRecursion = 0  -- Не идем вниз по таблицам из рекурсии
		begin

			if @DebugMsgsEnble = 1
				insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'DownTable start '+@Table)

			set @constri = null

			--цикл по таблицам которые ссылаются на объект
			while 1=1 begin
				--первое значение цикла 
				if @constri is null begin
					set @constri = (
						select min(ConstraintName) from @t
						where TableT = @Table and TableF <> @Table 
					)
				end
				--последущие значения цикла
				else begin
					set  @constri = (
						select top 1 ConstraintName from @t
						where TableT = @Table and TableF <> @Table 
						and ConstraintName > @constri						
						order by ConstraintName
					)
				end
				--условие выхода
				if @constri is null break
				
				--определить переменые цикла
				select @colF=columnF, @tbl = TableF
				from @t
				where ConstraintName=@constri

				if @DebugMsgsEnble = 1
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'DownTable @tbl- '+isnull(@tbl,'null')+' @constri-'+isnull(@constri,'null'))
				
				begin try 
					--найти id через PK 
					set @colT = null
					set @colT = (
						select c.name
						from sys.indexes i 
						join sys.index_columns ic on i.index_id = ic.index_id and i.object_id = ic.object_id
						join sys.columns c on ic.column_id = c.column_id and c.object_id = ic.object_id
						where i.object_id = object_id(@tbl) and i.is_primary_key=1
						and i.object_id in (-- не обрабатываем составные ключи (не придумал пока как)
							select i.object_id
							from sys.indexes i 
							join sys.index_columns ic on i.index_id = ic.index_id and i.object_id = ic.object_id
							where i.is_primary_key=1
							group by i.object_id
							having count(ic.column_id) = 1
						)
					)					

					if @colT is not null begin 
						--найти ключи для следующей таблицы
						delete from @NextTableKeys 
						delete from @TmpTableKeys

						insert into @TmpTableKeys 
						exec GetKeysIdTable
							 @DbName			= @dbSource
							,@TableName			= @tbl 
							,@ValueColumnName	= @colT 
							,@FilterColumnName	= @colF  
							,@FilterValues		= @InsideKeysId 
							,@lvl				= @lvl							
							,@TopRows			= @TopRowsi
							,@DebugMsgsEnble	= @DebugMsgsEnble
						insert into @NextTableKeys select distinct * from @TmpTableKeys											

						--запуск рекурсии
						set @lvl1 = @lvl+1
						exec MegaExport 
							 @dbSource = @dbSource
							,@dbTarget = @dbTarget
							,@Table = @tbl
							,@KeysColumn = @colT 
							,@KeysId = @NextTableKeys 
							,@constr = @constri
							,@lvl = @lvl1
							,@DebugMsgsEnble=@DebugMsgsEnble
							,@NoRecursion = 1 -- не идем глубже вниз по связям
							,@TopRows = @TopRowsi
							,@execute=@execute
					end 
				end try		
				begin catch
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'DownTable.ERROR proc-'+ERROR_PROCEDURE())
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'DownTable.ERROR line-'+cast(ERROR_LINE() as char))
					insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'DownTable.ERROR msg-'+ERROR_MESSAGE())	
				end catch 
			end
			if @DebugMsgsEnble = 1
				insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'DownTable end '+@Table)
		end 
	end 
	else begin		
		if @DebugMsgsEnble = 1 begin 		
			insert into MegaExportCmds(lvl,dbg,cmd) values (@lvl,1,'BeforeStart exit @Table- '+isnull(@Table,'null')+' @constr-'+isnull(@constr,'null'))
		end 		
	end 
end 
------------------------------------------------------------------------------------------------------------------------
go