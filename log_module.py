import pandas as pd
from datetime import datetime
from calendar import month_name,monthrange
import traceback
import inspect




class Log(object):
    def __init__(self) -> None:
        self.log_df=pd.DataFrame(columns=['date','log_type','function','plant','method_name','sub_sections','file','error','error_loc','status','processing_date'])
        self.today=""
        self.row_count=0
        self.pipeline_name=""
        self.cursor=""
        self.log_table=""
    
    def process_inspect_list(self,trace_list:list):
        return trace_list[0].function
        
    def capture_trace(self,trace:traceback,entity=None):
        if isinstance(trace,list):
            # print("Passed")
            func_name=self.process_inspect_list(trace)
            self.capture_log_info(func=func_name,entity=entity,error='NA',err_loc='NA',status='passed')
        else:
            err=repr(trace)
            err_loc=repr(traceback.extract_tb(trace.__traceback__))
            func_name=repr(traceback.extract_tb(trace.__traceback__)).split(' ')[-1][0:-2]
            self.capture_log_info(func=func_name,entity=entity,error=err,err_loc=err_loc,status='Failed')
            # print("Failed")
        
    def capture_log_info(self,func,error,err_loc,status,entity=None,sub_sec=None,file=None):
        # error_df=pd.DataFrame(columns=['date','log_type','function','plant','method_name','sub_sections','file','error','error_loc','status','processing_date'])
        self.log_df.loc[self.row_count,:]=(self.today,'Process Log',self.pipeline_name,entity,func,sub_sec,file,error,err_loc,status,datetime.now())
        self.row_count+=1
        # return error_df
    
    def wrap_log(self,org_func):
        def wrapper(*args,**kwargs):
            try:
                func=org_func(*args,**kwargs)
                self.capture_trace(inspect.stack()) 
            except Exception as exc:
                print(traceback.format_exc())
                self.capture_trace(exc)
            return func
        return wrapper
    
    def set_date(self,today):
        self.today=today
