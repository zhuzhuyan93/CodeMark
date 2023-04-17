# shellcheck disable=SC2164
environment=$1
start_date=$2
end_date=$3
# shellcheck disable=SC2209
yesterday=$(date -d "@$(($(date +%s) - 86400))" +'%Y-%m-%d')

# shellcheck disable=SC2236
if [ ! -n "$start_date" ]; then
  echo "Warning: No Input Start Date and End Date, Running Yesterday Data"
  start_date=$yesterday
  end_date=$yesterday
elif [ ! -n "$end_date" ]; then
  echo "Warning: NOT Input End Date, Running ${start_date} Data"
  end_date=$start_date
else
  echo "Correct Input"
fi

echo "Start Date: ${start_date}, End Date: ${end_date}" 

cd ./script/wideTable 
sh widerun.sh "$environment" "$start_date" "$end_date" 

if [[ $environment == *st ]]; then
    echo "ST Environment - Running End"
else
    cd ../aggTable
    sh aggrun.sh "$environment" "$start_date" "$end_date" 
    echo "ALL Running End." 
fi
