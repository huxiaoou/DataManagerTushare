#$bgn_date = "20120104"
#$stp_date = "20240812"
$bgn_date = "20241101"
$stp_date = "20241119"

python main.py --bgn $bgn_date --stp $stp_date download --switch fmd
python main.py --bgn $bgn_date --stp $stp_date download --switch contract
python main.py --bgn $bgn_date --stp $stp_date download --switch universe
python main.py --bgn $bgn_date --stp $stp_date download --switch position
python main.py --bgn $bgn_date --stp $stp_date download --switch basis
python main.py --bgn $bgn_date --stp $stp_date download --switch stock
#python main.py --bgn $bgn_date --stp $stp_date download --switch minute # update from juejin by month

python main.py --bgn $bgn_date --stp $stp_date update --switch fmd
python main.py --bgn $bgn_date --stp $stp_date update --switch position
python main.py --bgn $bgn_date --stp $stp_date update --switch basis
python main.py --bgn $bgn_date --stp $stp_date update --switch stock
