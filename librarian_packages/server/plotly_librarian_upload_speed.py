import plotly.plotly as py
import plotly.graph_objs as go
import json
from librarian_server import db
from librarian_server.file import FileEvent
import logging
logging.basicConfig(level=logging.ERROR)
rates = []
times = []
for item in db.session.query(FileEvent).\
        filter(db.func.age(FileEvent.time) < '30 day').\
        filter(FileEvent.type == 'copy_finished').\
        filter(FileEvent.name.like("%uvc")).\
        order_by(FileEvent.time).all():
    try:
        payload = json.loads(item.payload)
        rates.append(payload['average_rate'])  # kBps
    except(KeyError):
        # print "average rate not found"
        continue
    times.append(item.time)
print "importing", len(times), "data points"
annotations = []
annotations.append(dict(xref='paper', yref='paper', x=0.0, y=1.05,
                        xanchor='left', yanchor='bottom',
                        text='Upload speed',
                        font=dict(family='Arial',
                                  size=30,
                                  color='rgb(37,37,37)'),
                        showarrow=False))
layout = go.Layout(showlegend=True, title='Upload Speed',
                   yaxis={'title': 'kBps'})
data = [go.Scatter(x=times, y=rates, name='Karoo->UPenn (per file)')]
fig = go.Figure(data=data, layout=layout)
py.plot(fig, auto_open=False,
        filename='librarian_karoo_penn_upload_speed')  # ,fileopt='extend')
