#!/usr/bin/env python
CCTools datatypes

"""
import pkg_resources
pkg_resources.require( "bx-python" )
import gzip
import logging
import os
from cgi import escape
from galaxy import util
from galaxy.datatypes import data
from galaxy.datatypes import metadata
from galaxy.datatypes.checkers import is_gzip
from galaxy.datatypes.metadata import MetadataElement
from galaxy.datatypes.sniff import get_headers, get_test_fname
from galaxy.util.json import to_json_string
import dataproviders

log = logging.getLogger(__name__)

@dataproviders.decorators.has_dataproviders
class cctools( data.Text ):
	"""CCTools Log Data"""

	# All tabular data is chunkable.
	CHUNKABLE = True
	CHUNK_SIZE = 50000

	"""Add metadata elements"""
	MetadataElement( name="comment_lines", default=0, desc="Number of comment lines", readonly=False, optional=True, no_value=0 )
	MetadataElement( name="columns", default=0, desc="Number of columns", readonly=True, visible=False, no_value=0 )
	MetadataElement( name="column_types", default=[], desc="Column types", param=metadata.ColumnTypesParameter, readonly=True, visible=False, no_value=[] )
	MetadataElement( name="column_names", default=[], desc="Column names", readonly=True, visible=False, optional=True, no_value=[] )

	def init_meta( self, dataset, copy_from=None ):
		data.Text.init_meta( self, dataset, copy_from=copy_from )
	def set_meta( self, dataset, overwrite = True, skip = None, max_data_lines = 100000, max_guess_type_data_lines = None, **kwd ):
		"""
		Tries to determine the number of columns as well as those columns that
		contain numerical values in the dataset.  A skip parameter is used
		because various tabular data types reuse this function, and their data
		type classes are responsible to determine how many invalid comment
		lines should be skipped. Using None for skip will cause skip to be
		zero, but the first line will be processed as a header. A
		max_data_lines parameter is used because various tabular data types
		reuse this function, and their data type classes are responsible to
		determine how many data lines should be processed to ensure that the
		non-optional metadata parameters are properly set; if used, optional
		metadata parameters will be set to None, unless the entire file has
		already been read. Using None for max_data_lines will process all data
		lines.

		Items of interest:

		1. We treat 'overwrite' as always True (we always want to set tabular metadata when called).
		2. If a tabular file has no data, it will have one column of type 'str'.
		3. We used to check only the first 100 lines when setting metadata and this class's
		   set_peek() method read the entire file to determine the number of lines in the file.
		   Since metadata can now be processed on cluster nodes, we've merged the line count portion
		   of the set_peek() processing here, and we now check the entire contents of the file.
		"""
		# Store original skip value to check with later
		requested_skip = skip
		if skip is None:
			skip = 0
		column_type_set_order = [ 'int', 'float', 'list', 'str'  ] #Order to set column types in
		default_column_type = column_type_set_order[-1] # Default column type is lowest in list
		column_type_compare_order = list( column_type_set_order ) #Order to compare column types
		column_type_compare_order.reverse()
		def type_overrules_type( column_type1, column_type2 ):
			if column_type1 is None or column_type1 == column_type2:
				return False
			if column_type2 is None:
				return True
			for column_type in column_type_compare_order:
				if column_type1 == column_type:
					return True
				if column_type2 == column_type:
					return False
			#neither column type was found in our ordered list, this cannot happen
			raise "Tried to compare unknown column types"
		def is_int( column_text ):
			try:
				int( column_text )
				return True
			except:
				return False
		def is_float( column_text ):
			try:
				float( column_text )
				return True
			except:
				if column_text.strip().lower() == 'na':
					return True #na is special cased to be a float
				return False
		def is_list( column_text ):
			return "," in column_text
		def is_str( column_text ):
			#anything, except an empty string, is True
			if column_text == "":
				return False
			return True
		is_column_type = {} #Dict to store column type string to checking function
		for column_type in column_type_set_order:
			is_column_type[column_type] = locals()[ "is_%s" % ( column_type ) ]
		def guess_column_type( column_text ):
			for column_type in column_type_set_order:
				if is_column_type[column_type]( column_text ):
					return column_type
			return None
		data_lines = 0
		comment_lines = 0
		column_types = []
		first_line_column_types = [default_column_type] # default value is one column of type str
		if dataset.has_data():
			#NOTE: if skip > num_check_lines, we won't detect any metadata, and will use default
			dataset_fh = open( dataset.file_name )
			i = 0
			while True:
				line = dataset_fh.readline()
				if not line: break
				line = line.rstrip( '\r\n' )
				if i < skip or not line or line.startswith( '#' ):
					# We'll call blank lines comments
					comment_lines += 1
				else:
					data_lines += 1
					if max_guess_type_data_lines is None or data_lines <= max_guess_type_data_lines:
						fields = line.split()
						for field_count, field in enumerate( fields ):
							if field_count >= len( column_types ): #found a previously unknown column, we append None
								column_types.append( None )
							column_type = guess_column_type( field )
							if type_overrules_type( column_type, column_types[field_count] ):
								column_types[field_count] = column_type
					if i == 0 and requested_skip is None:
# This is our first line, people seem to like to upload files that have a header line, but do not
# start with '#' (i.e. all column types would then most likely be detected as str).  We will assume
# that the first line is always a header (this was previous behavior - it was always skipped).  When
# the requested skip is None, we only use the data from the first line if we have no other data for
# a column.  This is far from perfect, as
# 1,2,3	1.1	2.2	qwerty
# 0	0		1,2,3
# will be detected as
# "column_types": ["int", "int", "float", "list"]
# instead of
# "column_types": ["list", "float", "float", "str"]  *** would seem to be the 'Truth' by manual
# observation that the first line should be included as data.  The old method would have detected as
# "column_types": ["int", "int", "str", "list"]
						first_line_column_types = column_types
						column_types = [ None for col in first_line_column_types ]
				if max_data_lines is not None and data_lines >= max_data_lines:
					if dataset_fh.tell() != dataset.get_size():
						data_lines = None #Clear optional data_lines metadata value
						comment_lines = None #Clear optional comment_lines metadata value; additional comment lines could appear below this point
					break
				i += 1
			dataset_fh.close()

		#we error on the larger number of columns
		#first we pad our column_types by using data from first line
		if len( first_line_column_types ) > len( column_types ):
			for column_type in first_line_column_types[len( column_types ):]:
				column_types.append( column_type )
		#Now we fill any unknown (None) column_types with data from first line
		for i in range( len( column_types ) ):
			if column_types[i] is None:
				if len( first_line_column_types ) <= i or first_line_column_types[i] is None:
					column_types[i] = default_column_type
				else:
					column_types[i] = first_line_column_types[i]
		# Set the discovered metadata values for the dataset
		dataset.metadata.data_lines = data_lines
		dataset.metadata.comment_lines = comment_lines
		dataset.metadata.column_types = column_types
		dataset.metadata.columns = len( column_types )
	def make_html_table( self, dataset, **kwargs ):
		"""Create HTML table, used for displaying peek"""
		out = ['<table cellspacing="0" cellpadding="3">']
		try:
			out.append( self.make_html_peek_header( dataset, **kwargs ) )
			out.append( self.make_html_peek_rows( dataset, **kwargs ) )
			out.append( '</table>' )
			out = "".join( out )
		except Exception, exc:
			out = "Can't create peek %s" % str( exc )
		return out

	def make_html_peek_header( self, dataset, skipchars=None, column_names=None, column_number_format='%s', column_parameter_alias=None, **kwargs ):
		if skipchars is None:
			skipchars = []
		if column_names is None:
			column_names = []
		if column_parameter_alias is None:
			column_parameter_alias = {}
		out = []
		try:
			if not column_names and dataset.metadata.column_names:
				column_names = dataset.metadata.column_names

			column_headers = [None] * dataset.metadata.columns

			# fill in empty headers with data from column_names
			for i in range( min( dataset.metadata.columns, len( column_names ) ) ):
				if column_headers[i] is None and column_names[i] is not None:
					column_headers[i] = column_names[i]

			# fill in empty headers from ColumnParameters set in the metadata
			for name, spec in dataset.metadata.spec.items():
				if isinstance( spec.param, metadata.ColumnParameter ):
					try:
						i = int( getattr( dataset.metadata, name ) ) - 1
					except:
						i = -1
					if 0 <= i < dataset.metadata.columns and column_headers[i] is None:
						column_headers[i] = column_parameter_alias.get(name, name)

			out.append( '<tr>' )
			for i, header in enumerate( column_headers ):
				out.append( '<th>' )
				if header is None:
					out.append( column_number_format % str( i + 1 ) )
				else:
					out.append( '%s.%s' % ( str( i + 1 ), escape( header ) ) )
				out.append( '</th>' )
			out.append( '</tr>' )
		except Exception, exc:
			raise Exception, "Can't create peek header %s" % str( exc )
		return "".join( out )

	def make_html_peek_rows( self, dataset, skipchars=None, **kwargs ):
		if skipchars is None:
			skipchars = []
		out = []
		try:
			if not dataset.peek:
				dataset.set_peek()
			for line in dataset.peek.splitlines():
				if line.startswith( tuple( skipchars ) ):
					#out.append( '<tr><td colspan="100%%">%s</td></tr>' % escape( line ) )
					test = "test"
				elif line:
					elems = line.split( '\t' )
					# we may have an invalid comment line or invalid data
					if len( elems ) != dataset.metadata.columns:
					 #   out.append( '<tr><td colspan="100%%">%s</td></tr>' % escape( line ) )
						test = "test"
					else:
						out.append( '<tr>' )
						for elem in elems:
							out.append( '<td>%s</td>' % escape( elem ) )
						out.append( '</tr>' )
		except Exception, exc:
			raise Exception, "Can't create peek rows %s" % str( exc )
		return "".join( out )

	def get_chunk(self, trans, dataset, chunk):
		ck_index = int(chunk)
		f = open(dataset.file_name)
		f.seek(ck_index * self.CHUNK_SIZE)
		# If we aren't at the start of the file, seek to next newline.  Do this better eventually.
		if f.tell() != 0:
			cursor = f.read(1)
			while cursor and cursor != '\n':
				cursor = f.read(1)
		ck_data = f.read(self.CHUNK_SIZE)
		cursor = f.read(1)
		while cursor and ck_data[-1] != '\n':
			ck_data += cursor
			cursor = f.read(1)
		return to_json_string( { 'ck_data': util.unicodify( ck_data ), 'ck_index': ck_index + 1 } )

	def display_data(self, trans, dataset, preview=False, filename=None, to_ext=None, chunk=None):
		preview = util.string_as_bool( preview )
		if chunk:
			return self.get_chunk(trans, dataset, chunk)
		elif to_ext or not preview:
			return self._serve_raw(trans, dataset, to_ext)
		elif dataset.metadata.columns > 50:
			#Fancy tabular display is only suitable for datasets without an incredibly large number of columns.
			#We should add a new datatype 'matrix', with it's own draw method, suitable for this kind of data.
			#For now, default to the old behavior, ugly as it is.  Remove this after adding 'matrix'.
			max_peek_size = 1000000 # 1 MB
			if os.stat( dataset.file_name ).st_size < max_peek_size:
				return open( dataset.file_name )
			else:
				trans.response.set_content_type( "text/html" )
				return trans.stream_template_mako( "/dataset/large_file.mako",
											truncated_data = open( dataset.file_name ).read(max_peek_size),
											data = dataset)
		else:
			column_names = 'null'
			if dataset.metadata.column_names:
				column_names = dataset.metadata.column_names
			elif hasattr(dataset.datatype, 'column_names'):
				column_names = dataset.datatype.column_names
			column_types = dataset.metadata.column_types
			if not column_types:
				column_types = []
			column_number = dataset.metadata.columns
			if column_number is None:
				column_number = 'null'
			return trans.fill_template( "/dataset/tabular_chunked.mako",
						dataset = dataset,
						chunk = self.get_chunk(trans, dataset, 0),
						column_number = column_number,
						column_names = column_names,
						column_types = column_types )

	def set_peek( self, dataset, line_count=None, is_multi_byte=False):
		super(cctools, self).set_peek( dataset, line_count=line_count, is_multi_byte=is_multi_byte)
		if dataset.metadata.comment_lines:
			dataset.blurb = "%s, %s comments" % ( dataset.blurb, util.commaify( str( dataset.metadata.comment_lines ) ) )
	def display_peek( self, dataset ):
		"""Returns formatted html of peek"""
		return self.make_html_table( dataset )
	def displayable( self, dataset ):
		try:
			return dataset.has_data() \
				and dataset.state == dataset.states.OK \
				and dataset.metadata.columns > 0 \
				and dataset.metadata.data_lines != 0
		except:
			return False
	def as_gbrowse_display_file( self, dataset, **kwd ):
		return open( dataset.file_name )
	def as_ucsc_display_file( self, dataset, **kwd ):
		return open( dataset.file_name )

	def get_visualizations( self, dataset ):
		"""
		Returns a list of visualizations for datatype.
		"""
		# Can visualize tabular data as scatterplot if there are 2+ numerical
		# columns.
		num_numerical_cols = 0
		if dataset.metadata.column_types:
			for col_type in dataset.metadata.column_types:
				if col_type in [ 'int', 'float' ]:
					num_numerical_cols += 1

		vizs = super( cctools, self ).get_visualizations( dataset )
		if num_numerical_cols >= 2:
			vizs.append( 'scatterplot' )

		return  vizs

	# ------------- Dataproviders
	@dataproviders.decorators.dataprovider_factory( 'column', dataproviders.column.ColumnarDataProvider.settings )
	def column_dataprovider( self, dataset, **settings ):
		"""Uses column settings that are passed in"""
		dataset_source = dataproviders.dataset.DatasetDataProvider( dataset )
		return dataproviders.column.ColumnarDataProvider( dataset_source, **settings )

	@dataproviders.decorators.dataprovider_factory( 'dataset-column',
													dataproviders.column.ColumnarDataProvider.settings )
	def dataset_column_dataprovider( self, dataset, **settings ):
		"""Attempts to get column settings from dataset.metadata"""
		return dataproviders.dataset.DatasetColumnarDataProvider( dataset, **settings )

	@dataproviders.decorators.dataprovider_factory( 'dict', dataproviders.column.DictDataProvider.settings )
	def dict_dataprovider( self, dataset, **settings ):
		"""Uses column settings that are passed in"""
		dataset_source = dataproviders.dataset.DatasetDataProvider( dataset )
		return dataproviders.column.DictDataProvider( dataset_source, **settings )

	@dataproviders.decorators.dataprovider_factory( 'dataset-dict', dataproviders.column.DictDataProvider.settings )
	def dataset_dict_dataprovider( self, dataset, **settings ):
		"""Attempts to get column settings from dataset.metadata"""
		return dataproviders.dataset.DatasetDictDataProvider( dataset, **settings )


@dataproviders.decorators.has_dataproviders
class MakeflowLog( cctools ):
	file_ext = 'makeflowlog'
	skipchars = ['#']

	def __init__(self, **kwd):
		"""Initialize taxonomy datatype"""
		cctools.__init__( self, **kwd )
		self.column_names = ['Timestamp', 'NodeId', 'NewState', 'JobId',
				'NodesWaiting', 'NodesRunning', 'NodesComplete',
				'NodesFailed', 'NodesAborted', 'NodeIdCounter'
							 ]
	def display_peek( self, dataset ):
		"""Returns formated html of peek"""
		return cctools.make_html_table( self, dataset, column_names=self.column_names )

	def sniff( self, filename ):
		"""
		Determines whether the file is in MakeflowLog format

		>>> fname = get_test_fname( 'sequence.maf' )
		>>> MakeflowLog().sniff( fname )
		False
		>>> fname = get_test_fname( '1.makeflowlog' )
		>>> MakeflowLog().sniff( fname )
		True
		"""
		try:
			fh = open( filename )
			count = 0
			started = False
			while True:
				line = fh.readline()
				line = line.strip()
				if not line:
					break #EOF
				if line:
					linePieces = line.split('\t')
					if line[0] == '#':
						if linePieces[1] == 'STARTED':
							started = True
						elif linePieces[1] == 'COMPLETED':
							started = False
					elif started:
						if len(linePieces) < 10:
							return False
						try:
							check = int(linePieces[1])
							check = int(linePieces[2])
							check = int(linePieces[3])
							check = int(linePieces[4])
							check = int(linePieces[5])
							check = int(linePieces[6])
							check = int(linePieces[7])
							check = int(linePieces[8])
						except ValueError:
							return False
						count += 1
						if count == 5:
							return True
			fh.close()
			if count < 5 and count > 0:
				return True
		except:
			pass
		return False

	def set_meta( self, dataset, overwrite = True, skip = None, max_data_lines = 5, **kwd ):
		if dataset.has_data():
			dataset_fh = open( dataset.file_name )
			comment_lines = 0
			if self.max_optional_metadata_filesize >= 0 and dataset.get_size() > self.max_optional_metadata_filesize:
				# If the dataset is larger than optional_metadata, just count comment lines.
				for i, l in enumerate(dataset_fh):
					if l.startswith('#'):
						comment_lines += 1
					else:
						# No more comments, and the file is too big to look at the whole thing.  Give up.
						dataset.metadata.data_lines = None
						break
			else:
				# Otherwise, read the whole thing and set num data lines.
				for i, l in enumerate(dataset_fh):
					if l.startswith('#'):
						comment_lines += 1
				dataset.metadata.data_lines = i + 1 - comment_lines
			dataset_fh.close()
			dataset.metadata.comment_lines = comment_lines
			dataset.metadata.columns = 10
			dataset.metadata.column_types = ['str', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int']




@dataproviders.decorators.has_dataproviders
class WorkQueueLog( cctools ):
	file_ext = 'wqlog'
	skipchars = ['#']

	def __init__(self, **kwd):
		"""Initialize taxonomy datatype"""
		cctools.__init__( self, **kwd )
		self.column_names = ['Timestamp', 'TotalWorkersConnected', 'WorkersInit',
							 'WorkersIdle', 'WorkersBusy', 'TotalWorkersJoined',
							 'TotalWorkersRemoved', 'TasksWaiting', 'TasksRunning',
							 'TasksComplete', 'TotalTasksDispatched', 'TotalTasksComplete',
							 'TotalTasksCancelled', 'StartTime', 'TotalSendTime',
							 'TotalReceiveTime', 'TotalBytesSent', 'TotalBytesReceived',
							 'Efficiency', 'IdlePercentage', 'Capacity', 'Bandwidth',
							 'TotalCores', 'TotalMemory', 'TotalDisk', 'TotalGPUs',
							 'MinCores', 'MaxCores', 'MinMemory', 'MaxMemory',
							 'MinDisk', 'MaxDisk', 'MinGPUs', 'MaxGPUs'
							 ]
	def display_peek( self, dataset ):
		"""Returns formated html of peek"""
		return cctools.make_html_table( self, dataset, column_names=self.column_names )

	def sniff( self, filename ):
		"""
		Determines whether the file is in WorkQueue log format

		>>> fname = get_test_fname( 'sequence.wq' )
		>>> WorkQueueLog().sniff( fname )
		False
		>>> fname = get_test_fname( '1.wqlog' )
		>>> WorkQueueLog().sniff( fname )
		True
		"""
		try:
			fh = open( filename )
			count = 0
			while True:
				line = fh.readline()
				line = line.strip()
				if not line:
					break #EOF
				if line:
					if line[0] != '#':
						linePieces = line.split('\t')
						if len(linePieces) < 34:
							return False
						try:
							check = str(linePieces[1])
							check = int(linePieces[3])
							check = int(linePieces[4])
							check = int(linePieces[7])
							check = int(linePieces[8])
						except ValueError:
							return False
						count += 1
						if count == 5:
							return True
			fh.close()
			if count < 5 and count > 0:
				return True
		except:
			pass
		return False

	def set_meta( self, dataset, overwrite = True, skip = None, max_data_lines = 5, **kwd ):
		if dataset.has_data():
			dataset_fh = open( dataset.file_name )
			comment_lines = 0
			if self.max_optional_metadata_filesize >= 0 and dataset.get_size() > self.max_optional_metadata_filesize:
				# If the dataset is larger than optional_metadata, just count comment lines.
				for i, l in enumerate(dataset_fh):
					if l.startswith('#'):
						comment_lines += 1
					else:
						# No more comments, and the file is too big to look at the whole thing.  Give up.
						dataset.metadata.data_lines = None
						break
			else:
				# Otherwise, read the whole thing and set num data lines.
				for i, l in enumerate(dataset_fh):
					if l.startswith('#'):
						comment_lines += 1
				dataset.metadata.data_lines = i + 1 - comment_lines
			dataset_fh.close()
			dataset.metadata.comment_lines = comment_lines
			dataset.metadata.columns = 34
			dataset.metadata.column_types = ['str', 'int', 'int', 'int', 'int',
											 'int', 'int', 'int', 'int', 'int',
											 'int', 'int', 'int', 'int', 'int',
											 'int', 'int', 'int', 'int', 'int',
											 'int', 'int', 'int', 'int', 'int',
											 'int', 'int', 'int', 'int', 'int',
											 'int', 'int', 'int', 'int', 'int', 'int']
