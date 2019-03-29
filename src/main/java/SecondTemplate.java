//JavaPairRDD<String, Long> wordcountpairs = docs
//    // Map phase
//    .flatMapToPair((document) -> {
//      String[] tokens = document.split(" ");
//      HashMap<String, Long> counts = new HashMap<>();
//      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
//      for (String token : tokens) {
//        counts.put(token, 1L + counts.getOrDefault(token, 0L));
//      }
//      for (Map.Entry<String, Long> e : counts.entrySet()) {
//        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
//      }
//      return pairs.iterator();
//    })
//    // Reduce phase
//    .groupByKey()
//    .mapValues((it) -> {
//      long sum = 0;
//      for (long c : it) {
//        sum += c;
//      }
//      return sum;
//    });
