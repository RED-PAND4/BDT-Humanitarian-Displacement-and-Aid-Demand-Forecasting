# BDT-Humanitarian-Displacement-and-Aid-Demand-Forecasting

Design a system that combines forced-displacement data with contextual indicators to estimate short-term pressure on host regions and likely aid demand. The project could explore forecasting, geographic clustering, and resource prioritization.

Suggested datasets: UNHCR Refugee Data Finder / public API, UNHCR Operational Data Portal, other humanitarian context data where available.


## Possible Architecture

<table border="1">
  <tr>
    <td colspan="5" align="center"><strong>Docker - Containers</strong></td>
  </tr>
  <tr>
    <td bgcolor="#FFB3B3"><strong><font color="black">Data Ingestion</font></strong></td>
    <td bgcolor="#FFB347"><strong><font color="black">Data Storage</font></strong></td>
    <td bgcolor="#FFFF99"><strong><font color="black">Data Cleaning</font></strong></td>
    <td bgcolor="#90EE90"><strong><font color="black">Data Analysis & Forecasting</font></strong></td>
    <td bgcolor="#ADD8E6"><strong><font color="black">Data Visualisation</font></strong></td>
  </tr>
  <tr>
    <td>Apache Kafka</td>
    <td>Delta Lake</td>
    <td>Dask</td>
    <td>Spark Streaming</td>
    <td>Redis</td>
  </tr>
</table>


### Questions:
- Delta lakes
- Are these 5 containers enough?
- What we use as orchestrator?
- What is the rate of refresh for the part about forecast? And for the Analysis?
- How we make the containers communicate with each other?