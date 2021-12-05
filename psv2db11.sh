
echo -n ~${1%.*}~ | sed 's/.*\///g' | sed 's/\.cvs//g'

#echo -n ~${1#*/}~ | sed 's/\.cvs//g'

head -n 1 $1 | sed 's/|/~/g' 

tail -n +2 $1 | sed 's/|/\t/g'
#echo
echo @

 
