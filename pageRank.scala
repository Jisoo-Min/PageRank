#! /bin/sh
exec scala "$0" "$@"
!#

object pageRank{
	def main(args: Array[String]){

		val data = sc.textFile("/dataset/web-Google.txt")

		val googleWeblinks = data.filter(!_.contains("#")).map(_.split("\t")).map(x=>(x(0), x(1)))

		val links = googleWeblinks.groupByKey.cache()

		var ranks = links.mapValues(v=>1.0)

		val iters = 3

		for ( i <- 1 to iters){
			val contribs = links.join(ranks).values.flatMap{case (urls, rank) => 
			val size = urls.size
			urls.map(url => (url, rank / size))
			}

			ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85*_)
		}

		ranks.take(10)
	}
}

pageRank.main(args)
