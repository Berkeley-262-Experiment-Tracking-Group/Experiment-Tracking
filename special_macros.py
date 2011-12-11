import dag
import os
#List of macros
macro_list=['produce_output_list_macro']

# Merely creates a file containing all output directories.
def produce_output_list_macro(node):
    
    output_path=os.path.join(node.exp_results, 'out')
    
    with open(output_path, 'w') as f:
        for x in node.parents:
            f.write(x.hsh)
            f.write('\n')

def evaluate(macro_str, node):
    
    if(macro_str not in macro_list):
        print 'Unknown macro:{}. Aborting.'.format(macro_str)
        exit(1)
        
    eval(macro_str+'(node)')
    
    return 0          
            


