import dag
import os
import csv

#List of macros
macro_list=['produce_output_list_macro', 'produce_annotated_list_macro', 'produce_parameter_map_macro', 'produce_all_map_macro', 'compute_percentiles_macro']

# Creates a file containing all output directories.
def produce_output_list_macro(node):
    
    output_path=os.path.join(node.exp_results, 'out')
    
    with open(output_path, 'w') as f:
        for x in node.parents:
            f.write(x.exp_results)
            f.write('\n')

def produce_annotated_list_macro(node, param_name):
    output_path=os.path.join(node.exp_results, 'annot_out')
    
    with open(output_path, 'w') as f:
        for x in node.parents:
            f.write(str(x.params[param_name])+' : '+x.exp_results)
            f.write('\n')

# This macro assumes that each parent job has written its output
# (typically a single number, or multiple numbers separated by spaces)
# to the first line of a file "out" in its results directory. 
def produce_parameter_map_macro(node, param_name):
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


def produce_all_map_macro(node, infile, outfile):
    output_path=os.path.join(node.exp_results, outfile)
    header=False
    with open(output_path, 'w') as f:
        print "writing parameter map to file %s ..." % (output_path),
        for x in node.parents:
            try:
                fi = open(os.path.join(x.exp_results, infile), 'r')
            except IOError:
                print "Error: could not open input file '%s' from job '%s'" % (os.path.join(x.exp_results, infile), x.info['description'])
                exit(1)
            if not header:
                f.write('# ')
                for param_key in x.params:
                    f.write(param_key+' ')
                f.write('output_val\n')
                header=True
                
            for param_key in x.params:
                val=x.params[param_key]
                if(isinstance(val,type('string'))):
                    f.write('"'+val+'" ')
                else:
                    f.write(str(val)+' ')
            param_val = list(fi)[0].strip()
            fi.close()
            f.write(param_val)
            f.write('\n')
        f.close()
        print "done."

def compute_percentiles_macro(node, infile, outfile, xaxiscols, yaxiscol, low_percentile, high_percentile):

    low_percentile = low_percentile / 100 if low_percentile >= 1 else low_percentile
    high_percentile = high_percentile / 100 if high_percentile >= 1 else high_percentile

    print "starting ebars macro"

    data = dict()
    fin = open(infile, 'rb')
    reader = csv.reader(fin, delimiter=' ', quotechar='\"')

    print "opened"

    colnames = reader.next()
    for row in reader:
        label = ""
        for col in xaxiscols:
            label += row[colnames.index(col) -1] + ", "
        label = label[:-2]
        if label not in data:
            data[label] = []
        data[label] += [float(row[colnames.index(yaxiscol) -1])]
    fin.close()

    print "read"

    fout = open(os.path.join(node.exp_results, outfile), 'w')
    writer = csv.writer(fout, delimiter=' ', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
    for (idx, (label, values)) in enumerate(sorted(data.items())):
        s = sorted(values)
        bottom = s[int(len(s) * low_percentile)]
        top = s[int(len(s) * high_percentile)]
        med = s[int(len(s) * 0.5)]
        writer.writerow(idx, label, med, bottom, top)

    print "written"
    fout.close()


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
        
    print "running macro", macro_str
    eval(macro_str.replace('(', '(node, ', 1))
    
    return 0          
            


