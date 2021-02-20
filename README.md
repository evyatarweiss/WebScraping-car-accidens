# WebScraping-car-accidens israel
##(1)The code wont work if you dont change the variables to your own GCP Project 
and also you need to bring your own json key to the project.
Web Scraping - 
Automate download the most updated pdf file from https://www.gov.il/he/departments/general/daily_report.
after downloading automaticly using Selenium and BeautifulSoup python libraries my goal was to create a onw row from each pdf file.
so it had to go throw some proccessing.
in orer to add it to BIG-QUERY Google-Cloud-Platform table, we had to transform the pdf-file using tabula-py
which takes an pdf and transform it into list of dataframes.
(2)in order to use Tabula-py you need to set your own enviorment variable in computer/text editor of Java.
and also Java has to be installed.
For example in my computer - 
"JAVA_HOME" C:\Program Files\Java\jdk-15.0.1

Enjoy.
