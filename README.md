# Time Series Test

Function **mvn_historical_drawdowns** takes ***Asset Code***, ***Price Type***, 
***Period_Start***, ***Period_End*** and ***Rank*** as an input.

**Expected Output**:
1. Drawdown Start Date
2. Drawdown End Date
3. Drawdown Performance
4. Recovery Periods.

**Workflow**:
- Read data from server using function **read_data** for the provided **Asset Code** 
and **Price Type**.
    - In case the Asset Code or Price Type is not available an expection
    is raised.
-  Period start and end date provided by the user is then parsed 
using function **parse_dates**.
    - If user leaves period start and end date empty in that case
    all available data in DB will be used for further calculations.
    - User has an option to provide an offset of Daily (D), Monthly (M)
    Weekly (W) and Yearly (Y).
    - If an offset is provided for Period end date it is derived 
    using latest available date in DB for that Asset Code and Price Type.
    - If an offset is provide for Period start date it is derived using
    Period end date hence in this case Period Start depends on Period End.
    - User has an option to provide both start and end date as a string or Date.