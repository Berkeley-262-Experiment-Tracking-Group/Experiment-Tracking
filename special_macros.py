import dag
import os
#List of macros
macro_list=['produce_output_list_macro', 'produce_annotated_list_macro']




# Creates a file containing all output directories.
def produce_output_list_macro(node):
    
    output_path=os.path.join(node.exp_results, 'out')
    
    with open(output_path, 'w') as f:
        for x in node.parents:
            f.write(x.exp_results)
            f.write('\n')

def produce_annotated_list_macro(param_name, node):
    output_path=os.path.join(node.exp_results, 'annot_out')
    
    with open(output_path, 'w') as f:
        for x in node.parents:
            f.write(str(x.params[param_name])+' : '+x.exp_results)
            f.write('\n')


def check_for_macro(macro_str):
    parts=macro_str.partition('(')
    if(parts[0] not in macro_list):
        return False
    else:
        return True

def evaluate(macro_str, node):
    
    
    if(not check_for_macro(macro_str)):
        print 'Unknown macro:{}. Aborting.'.format(macro_str)
        exit(1)
        
    eval(macro_str.format('node'))
    
    return 0          
            


