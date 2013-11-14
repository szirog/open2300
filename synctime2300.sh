#!/bin/sh
# Set WS-23XX station time to system localtime

WS_W_Year=`date +%y`
WS_W_Month=`date +%m`
WS_W_Day=`date +%d`
WS_W_Hour=`date +%H`
WS_W_Minute=`date +%M`
WS_W_Second=`date +%S`

OPEN2300PATH=/media/USB_4GB/open2300
OPEN2300APP=open2300

WS_W_YearH=$(($WS_W_Year * 10 / 100))
WS_W_YearL=$(($WS_W_Year - $(($WS_W_Year * 10 / 100)) * 10))
WS_W_MonthH=$(($WS_W_Month * 10 / 100))
WS_W_MonthL=$(($WS_W_Month - $(($WS_W_Month * 10 / 100)) * 10))
WS_W_DayH=$(($WS_W_Day * 10 / 100))
WS_W_DayL=$(($WS_W_Day - $(($WS_W_Day * 10 / 100)) * 10))
WS_W_HourH=$(($WS_W_Hour * 10 / 100))
WS_W_HourL=$(($WS_W_Hour - $(($WS_W_Hour * 10 / 100)) * 10))
WS_W_MinuteH=$(($WS_W_Minute * 10 / 100))
WS_W_MinuteL=$(($WS_W_Minute - $(($WS_W_Minute * 10 / 100)) * 10))
WS_W_SecondH=$(($WS_W_Second * 10 / 100))
WS_W_SecondL=$(($WS_W_Second - $(($WS_W_Second * 10 / 100)) * 10))

echo "Current date-time:"
echo $WS_W_Year $WS_W_YearH $WS_W_YearL
echo $WS_W_Month $WS_W_MonthH $WS_W_MonthL
echo $WS_W_Day $WS_W_DayH $WS_W_DayL
echo $WS_W_Hour $WS_W_HourH $WS_W_HourL
echo $WS_W_Minute $WS_W_MinuteH $WS_W_MinuteL
echo $WS_W_Second $WS_W_SecondH $WS_W_SecondL

# 0200 0    Current time: Seconds BCD 1s
# 0201 1    Current time: Seconds BCD 10s
# 0202 6    Current time: Minutes BCD 1s
# 0203 2    Current time: Minutes BCD 10s
# 0204 3    Current time: Hours BCD 1s
# 0205 0    Current time: Hours BCD 10s

$OPEN2300PATH/$OPEN2300APP 0200 w $WS_W_SecondL$WS_W_SecondH$WS_W_MinuteL$WS_W_MinuteH$WS_W_HourL$WS_W_HourH
#$OPEN2300PATH/$OPEN2300APP 0200 r 3

# 024D 1    Date the unit was last set to: BCD 1s
# 024E 2    Date the unit was last set to: BCD 10s
# 024F 4    Month the unit was last set to: BCD 1s
# 0250 0    Month the unit was last set to: BCD 10s
# 0251 3    Year the unit was last set to: BCD 1s
# 0252 0    Year the unit was last set to: BCD 10s

$OPEN2300PATH/$OPEN2300APP 024D w $WS_W_DayL$WS_W_DayH$WS_W_MonthL$WS_W_MonthH$WS_W_YearL$WS_W_YearH
#$OPEN2300PATH/$OPEN2300APP 024D r 3 

# 023B 6    Current Time: Minutes BCD 1s
# 023C 2    Current Time: Minutes BCD 10s
# 023D 3    Current Time: Hours BCD 1s
# 023E 0    Current Time: Hours BCD 10s
# 023F 4    Current Weekday: Mon-Sun = 1-7
# 0240 4    Current day of month: BCD 1s
# 0241 2    Current day of month: BCD 10s
# 0242 4    Current month: BCD 1s
# 0243 0    Current month: BCD 10s
# 0244 3    Current year: BCD 1s last two digits
# 0245 0    Current year: BCD 10s last two digit

$OPEN2300PATH/$OPEN2300APP 023B r 6


exit 0;


