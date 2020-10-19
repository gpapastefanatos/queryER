
python generate_people_full.py full500k5p.csv 475000 25000 3 2 4 poisson False
python generate_people_full.py full500k15p.csv 425000 75000 3 2 4 poisson False
python generate_people_full.py full500k25p.csv 375000 125000 3 2 4 poisson False
python generate_people_full.py full500k35p.csv 325000 175000 3 2 4 poisson False
python generate_people_full.py full500k45p.csv 275000 225000 3 2 4 poisson False


mkdir ../../resources/people/
mkdir ../../resources/people/ground_truth
mv full*.csv ../../resources/people/
mv ground_truth*.csv ../../resources/people/ground_truth
cd ../../ 
