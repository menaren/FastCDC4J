package de.zabuza.fastcdc4j.internal.chunking.fastcdc;

import de.zabuza.fastcdc4j.external.chunking.IterativeStreamChunkerCore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class FastCdcChunkerCore implements IterativeStreamChunkerCore {
	// TODO Make configurable
	private static final int EXPECTED_SIZE = 8 * 1024;
	private static final long[] GEAR =
			{0x5a16b18f2aac863eL, 0x05fad735784f09eaL, 0x355c6a3868fe64afL, 0x57df89c95716c702L, 0x46ea7572135544a6L,
					0x6291d5376cd79d73L, 0x2a6e072b609b0bbfL, 0x110f7f895ec438b7L, 0x2fc580f60659f690L,
					0x15ce33c924a8880bL, 0x1f3fabc44c091f5fL, 0x76e7512d0f53c142L, 0x30ff6d65448b44b3L,
					0x16db576e7ecfe3c9L, 0x7009bea841de2e20L, 0x0ad460d80f3fe181L, 0x0a1e6fed6ece42dbL,
					0x310d45cf0b0fdf5fL, 0x08a96d563dbf589eL, 0x0242bfc35fec28d5L, 0x627c8f391a430cf8L,
					0x6f6615ff6f6de700L, 0x6f8979754e271c9dL, 0x0859f64c2f67e948L, 0x63c2fd593e2f50cdL,
					0x36841c4d612cf902L, 0x10771d5b54e6391aL, 0x49b9ec670a771b9dL, 0x40bd85df748b9357L,
					0x2515b6965ec755c6L, 0x6e9fed0765bc944eL, 0x545b14b62bb0033eL, 0x1ffb4acd721cfb37L,
					0x2e11a5484f7e4d76L, 0x513cfa1d1409599fL, 0x667fc32675a9d9baL, 0x4b900ca83c6a130eL,
					0x6c883f7266b43d29L, 0x113f68df448dfb9bL, 0x610e6c284b1063c9L, 0x7ea51d3447a18deeL,
					0x1a0330843df0bbdbL, 0x539425db2defa9a3L, 0x6d516c376fad3325L, 0x3cc9a49e4e0b7534L,
					0x50a3d0312106b015L, 0x70fd64403ee483a5L, 0x7f789f326a75f2bdL, 0x7afca98213354a68L,
					0x023fedf465463724L, 0x79b02c7e2da5aa52L, 0x24c3c25b46d23042L, 0x0a6211f96f3f7007L,
					0x14d25f9364aed60bL, 0x232bbe945ecae09dL, 0x01bfcf37345dfd8dL, 0x01e47a9f5ee39461L,
					0x68faae243200a1ecL, 0x6c0a62fb6fca808cL, 0x79c5111c0a4f01d4L, 0x57e629f053268e22L,
					0x6637da42628a1d23L, 0x3a77374201bdd718L, 0x357af0823b8b08cfL, 0x6efd96b521cabfe4L,
					0x6406490661488eb8L, 0x2552e2ee3a5bc8d0L, 0x072b854c221af964L, 0x6dcef4e9443f337cL,
					0x0091f9a034af01cdL, 0x716fd3b3054e0bd0L, 0x1f5fbc1c6eb165c5L, 0x60e6856b540b51efL,
					0x1f11114e7c484faeL, 0x0000d09e058d4ad6L, 0x18e2758b749c1f10L, 0x75e885ce5eed60ccL,
					0x186390bc4089d41bL, 0x1a106a271448c1b7L, 0x0683a2b974697cf6L, 0x7c1fa0de32f4b108L,
					0x43c256022118b41aL, 0x0ad3169d2f4332eaL, 0x3de6dd9469e6b4cbL, 0x03214ec53d08b415L,
					0x47719437112e7492L, 0x54a5a37e24f2ecc0L, 0x1e672b6539cbdc95L, 0x63710b3b7c6a9138L,
					0x261906b85d188087L, 0x01a645217ab48b46L, 0x600aca562207be19L, 0x01c53fb56d5baabdL,
					0x6f77743452e4242bL, 0x62c146d02b9d58a8L, 0x4737fd8369b2ba58L, 0x440d74f45afe4f58L,
					0x309e12a057084cfbL, 0x19799d1500ddb160L, 0x0b4f9faf35698814L, 0x06ae60c0031acc89L,
					0x0f0bd5c06074337cL, 0x3280bd843dbfe2b4L, 0x79aa26532817b7d9L, 0x04c3231906d36fe7L,
					0x694a4eaa6f6d5f16L, 0x6971b68d494d1c6aL, 0x09f542094e8cc44cL, 0x02d7df3649d5ab34L,
					0x19b56823600284d6L, 0x333f0da127d2627dL, 0x6fad1e187fdd5340L, 0x0262e81653eade70L,
					0x4d544dad66a68a81L, 0x076d264a36c5dd17L, 0x78245a80673d88efL, 0x6e0a18da10b610e2L,
					0x3c32a91c5fdd6cc0L, 0x389edc3f5e3cc958L, 0x45b75df31f4a782dL, 0x5d1e968117a2ebdcL,
					0x2dc2ab5a0b68a79fL, 0x32e720973e523772L, 0x0bc636021a497728L, 0x1764a6b63fa13424L,
					0x22eb06cd5dc350feL, 0x2e8a233e69f6b146L, 0x5e58a3d53bd96d8bL, 0x694e34753307b070L,
					0x13ff2d4025effd64L, 0x47dab89f3a59b9aeL, 0x6bf66763145942afL, 0x0bdbbe1f443e0a55L,
					0x61fcf9652a245b4cL, 0x6a19a16d4be82702L, 0x3ad88e2e7482fef7L, 0x1b2aee5b64b1a96aL,
					0x476fa0f04de1ad0bL, 0x391770c140f198e7L, 0x2f521a79172b0d05L, 0x65864b7d3bed5d71L,
					0x785d0c7a77073370L, 0x48b96fc44b4849d1L, 0x741e87224ec165f3L, 0x4b67d9ca5b638351L,
					0x3e96913f2bbd6ed0L, 0x5afa027e3fb5a1e9L, 0x5b775fc205742763L, 0x4ed6cca60f781be3L,
					0x0de489eb0e32be40L, 0x0e50a4cd4dd4cd25L, 0x5193b326399d128cL, 0x105689474d7a367bL,
					0x6e99487e230e2428L, 0x4d54e9d05fd41632L, 0x587d30714e09df98L, 0x338578c64a0576a4L,
					0x5285f2b6253c8148L, 0x539bed9a237ea4b6L, 0x4cf4ef9c38c68c42L, 0x17c8be100cc5a8f3L,
					0x4cdc53e77f800cc1L, 0x604a1cc979463cacL, 0x2f2644603f5ad2f7L, 0x521bed84059390a7L,
					0x0a6a4d9131ae8eaeL, 0x37ee95f81465f059L, 0x6b5a472466c28910L, 0x64d391ba1ba64cfcL,
					0x2f9416a533ad2fe2L, 0x0ed532ff334c32dbL, 0x6c1ff320704db444L, 0x6cfddaa55a0c7e01L,
					0x7b9111f72d8db30dL, 0x1639e4ee0e2b053eL, 0x4b7ca73a4b5c22daL, 0x7b20510d45cc0b67L,
					0x04fd7dcb77857ba2L, 0x655190f26e03692aL, 0x722c89944077844cL, 0x7f990ad975187f50L,
					0x6dc7791921fafce9L, 0x2545283c54bfe0d2L, 0x138088684f05e2d1L, 0x373dcce0457d4a4dL,
					0x2f0a9f54154ad0ffL, 0x574f8d4818d0ffb8L, 0x50f1d3e804c93f0eL, 0x5fbd2b8c07db45f0L,
					0x691960cf770fdff8L, 0x31db755513e8d39cL, 0x1246e1003b30b0d5L, 0x5837c71a1501ea70L,
					0x67db6a3254bf1b57L, 0x3eb810bf66824135L, 0x78462ce501fd3358L, 0x079f7e61300bc19bL,
					0x0a63c6be07c9cbb7L, 0x012b3d262a85220cL, 0x365cb5ed3547ece6L, 0x24c194923ca64947L,
					0x60f1d96b430fa7e6L, 0x387badc5327750eaL, 0x01de5341400e9e27L, 0x03b3361536553067L,
					0x1a5e898a04dc5bd2L, 0x2a16207973e1c917L, 0x13b80dee498e46f6L, 0x282b6cb267f3ffc9L,
					0x1aa573c93edaa6ccL, 0x4724f7df33c54d9cL, 0x7b6470f71e0110c2L, 0x0eea5ab21cdf1d21L,
					0x169dce076f100ac5L, 0x3cb120ab66166d34L, 0x02ee08213534e408L, 0x204b064c0324391aL,
					0x1a55e5ef256f6050L, 0x228903333cc8b196L, 0x73c5fc5324fccf9bL, 0x1debf745427a832eL,
					0x2b38e7fe56472ce5L, 0x2c69b6592288b8ceL, 0x3e6a912353b96e40L, 0x0a6b2a4f6b1a413dL,
					0x09f0f0c2632b2016L, 0x5fa2dcc943d7bc78L, 0x76f44ebe0c29d7e4L, 0x7d85baca0aed3fb4L,
					0x562a53fd09ae5f20L, 0x0d836313270f989dL, 0x475a18725f305dc6L, 0x7893ff4e7f495128L,
					0x25fe3b134f4a865aL, 0x4c7f96e43712cba8L, 0x29caf4d32c543d7cL, 0x5b99aa853e46d124L,
					0x07a3e5aa538635e5L, 0x0e4b57d8155f9c88L, 0x2f71282b3c4fa210L, 0x798c81923d71487fL,
					0x45e90bc2494ad436L, 0x5291bcf62f0b6bdbL, 0x72ea193619f06853L, 0x5a5a2bd77114b311L,
					0x5445faa82e02e158L, 0x0065712926726beaL, 0x1bed3b9a62fbf757L, 0x1767b815257b83d4L,
					0x000eab4e77327b81L, 0x0fd333301966ff16L, 0x6780eb8339b83286L, 0x7652a5e647799673L,
					0x43c0db665e364315L, 0x6fe4fe01606d405dL, 0x6833dbd876b03920L};

	private static final long MASK_L = 0b110110010000000000000011010100110000000000000000L;
	private static final long MASK_S = 0b11010110010000011100000011010100110000000000000000L;
	private static final int MAX_SIZE = 64 * 1024;
	private static final int MIN_SIZE = 2 * 1024;

	@Override
	public byte[] readNextChunk(final InputStream stream, final long size, final long currentOffset) {
		try {
			int normalSize = EXPECTED_SIZE;
			long n = size - currentOffset;
			if (n <= 0) {
				throw new IllegalArgumentException();
			}
			if (n <= MIN_SIZE) {
				return stream.readNBytes((int) n);
			}
			if (n >= MAX_SIZE) {
				n = MAX_SIZE;
			} else if (n <= normalSize) {
				normalSize = (int) n;
			}

			ByteArrayOutputStream dataBuffer = new ByteArrayOutputStream();
			long fingerprint = 0;
			int i = MIN_SIZE;
			dataBuffer.write(stream.readNBytes(i));

			for (; i < normalSize; i++) {
				int data = stream.read();
				if (data == -1) {
					throw new IllegalStateException();
				}
				dataBuffer.write(data);
				fingerprint = (fingerprint << 1) + GEAR[data];
				if ((fingerprint & MASK_S) == 0) {
					return dataBuffer.toByteArray();
				}
			}
			for (; i < n; i++) {
				int data = stream.read();
				if (data == -1) {
					throw new IllegalStateException();
				}
				dataBuffer.write(data);
				fingerprint = (fingerprint << 1) + GEAR[data];
				if ((fingerprint & MASK_L) == 0) {
					return dataBuffer.toByteArray();
				}
			}

			return dataBuffer.toByteArray();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
