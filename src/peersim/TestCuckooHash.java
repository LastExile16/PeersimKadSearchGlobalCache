package peersim;

public class TestCuckooHash {

	public static void main(String[] args) {

		System.out.println("Hello Cuckoo!");
		long cumulative = 0;

		final int NUMS = 2000000;
		final int GAP  =   37;
		final int ATTEMPTS = 10;

		System.out.println( "Checking... (no more output means success)" );

		for( int att  = 0; att < ATTEMPTS; att++ )
		{
			System.out.println( "ATTEMPT: " + att );

			CuckooHashMap<String, String> H = new CuckooHashMap<>();

			long startTime = System.currentTimeMillis( );

			for( int i = GAP; i != 0; i = ( i + GAP ) % NUMS )
				H.put( ""+i, null );
			
			for( int i = 1; i < NUMS; i+= 2 )
				H.remove( ""+i );

			for( int i = 2; i < NUMS; i+=2 )
				if( !H.containsKey( ""+i ) )
					System.out.println( "Find fails " + i );

			for( int i = 1; i < NUMS; i+=2 )
			{
				if( H.containsKey( ""+i ) )
					System.out.println( "OOPS!!! " +  i  );
			}


			long endTime = System.currentTimeMillis( );

			cumulative += endTime - startTime;

			if( H.size( ) > NUMS * 4 )
				System.out.println( "LARGE size " + H.size( ) );
		}

		System.out.println( "Total elapsed time is: " + cumulative );
	}

}