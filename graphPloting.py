from bokeh.plotting import curdoc
from bokeh.plotting import Figure
import numpy as np
from bokeh.models import ColumnDataSource
import os
import csv
import datetime

source = ColumnDataSource(dict(x=[], y=[]))
fig = Figure()

fig.line(source=source, x='x', y='y', line_width=2, alpha=0.85, color='red')


ct = 0
sine_sum = 0
accessTime = None
your_list = None
def update_data():
    global ct, sine_sum, accessTime, your_list
    xd = 0
    yd = 0
    ct += 1
    time = datetime.datetime(100,1,1,11,)
    sine = np.sin(ct)
    sine_sum += sine
    if os.path.getmtime('./DATA FOR BOKEH/data.csv') != accessTime:
        accessTime = os.path.getmtime('./DATA FOR BOKEH/data.csv')
        with open('./DATA FOR BOKEH/data.csv', 'r') as f:
            reader = csv.reader(f)
            your_list = list(reader)
            print(your_list)
            print('len of list ', str(len(your_list)))
            if your_list[0][2] == '':
                xd = 0
            else:
                xd = your_list[0][2]
            yd = your_list[0][5]
            f.close()
            #datetime.datetime.time(datetime.datetime.now()).strftime('%I:%M %p')
    new_data = dict(x=[ct], y=[xd])
    print(new_data)
    source.stream(new_data, 80)

curdoc().add_root(fig)
curdoc().add_periodic_callback(update_data, 1000)