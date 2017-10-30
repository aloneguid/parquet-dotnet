val s = System.nanoTime

def msFrom(start: Long) : Double = {
   val diff = System.nanoTime - start
   val ms = diff/1e6
   ms
}

msFrom(s)


