#! /bin/bash
#sends data from the librarian to a plotly dashboard
source /home/obs/.bashrc;
cd ~/src/librarian/server;
python plotly_librarian_upload_speed.py >> /dev/null 2>/dev/null
