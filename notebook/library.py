import os
import pandas as pd



class DataLibrary:
    def __init__ ( self, directory, library_filename ):
        self.directory = directory
        self.library_filename = library_filename

        if not os.path.exists( self.get_library_filename() ):
            self.library = pd.DataFrame()
            self.save_library()

        try:
            self.library = pd.read_csv( self.get_library_filename() )

        except:
            self.library = pd.DataFrame()

    def clear_library ( self ):
        if len( self.library ) == 0:
            return
        
        for filename in self.library._filename:
            os.remove( filename )
            
        self.library = pd.DataFrame()
        self.save_library()

    def get_library_filename ( self ):
        return f'{self.directory}/{self.library_filename}'
    
    def get_next_data_filename ( self ):
        filename = lambda suffix: f'data-{suffix:05d}'
        suffix = 0

        while os.path.exists( self.get_qualified_filename( suffix ) ):
            suffix += 1

        return filename( suffix )

    def get_qualified_filename ( self, filename ):
        return f'{self.directory}/{filename}'
    
    #
    # Assumes it's going to be passed a GeoDataFrame
    #
    def save_data ( self, data, **kwargs ):
        filename = self.get_next_data_filename()
        data.to_file( self.get_qualified_filename( filename ), driver = 'GeoJSON' )

        self.library = pd.concat(
            [
                self.library,
                pd.DataFrame.from_records( [ { **kwargs, '_filename': filename } ] )
            ],
            axis = 0,
            ignore_index = True
        )
        self.save_library()

    def save_library ( self ):
        self.library.to_csv( self.get_library_filename(), index = False )