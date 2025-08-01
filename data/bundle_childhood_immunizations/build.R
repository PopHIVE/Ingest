# read data from data source projects
# and write to this project's `dist` directory

schoolvaxview <- vroom::vroom('../schoolvaxview/standard/data.csv.gz')

schoolvaxview_exempt <- vroom::vroom('../schoolvaxview/standard/data.csv.gz')

nis <- vroom::vroom('../nis/standard/data.csv.gz')

epic <- vroom::vroom('../epic/standard/children.csv.gz')
