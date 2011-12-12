import dag
import os
#List of macros
macro_list=['produce_output_list_macro', 'produce_annotated_list_macro', 'produce_parameter_map_macro']

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

# This macro assumes that each parent job has written its output
# (typically a single number, or multiple numbers separated by spaces)
# to the first line of a file "out" in its results directory. 
def produce_parameter_map_macro(param_name, node):
    output_path=os.path.join(node.exp_results, 'param_out')
    
    with open(output_path, 'w') as f:
        print "writing parameter map to file %s ..." % (output_path),
        for x in node.parents:
            try:
                fi = open(os.path.join(x.exp_results, 'out'), 'r')
            except IOError:
                print "Error: could not open output file '%s' from job '%s'" % (os.path.join(x.exp_results, 'out'), x.info['description'])
                exit(1)
            param_val = list(fi)[0].strip()
            fi.close()
            f.write(str(x.params[param_name])+' '+ param_val)
            f.write('\n')
        f.close()
        print "done."

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
            


