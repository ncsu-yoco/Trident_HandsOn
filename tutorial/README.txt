- Please make sure you have unzipped the tutorial folder to location '~/apache-storm-0.9.3/examples/storm-starter/src/jvm/storm/starter/trident/'

- Open terminal and change your current working directory to directory where pom.xml resides, i.e. '~/apache-storm-0.9.3/examples/storm-starter'

- Run following command on terminal to compile and include dependencies [ if any ]
$ mvn package

- Make sure correct source file 'stocks.csv.gz' is present under directory '~/apache-storm-0.9.3/examples/storm-starter/data'

- Run topology using following command
$ storm jar target/storm-starter-0.9.3-jar-with-dependencies.jar storm.starter.trident.tutorial.DRPCStockQueryTopology > op.txt

- To verify output please run below command
$ cat op.txt | grep DRPC
