import React from 'react'
import { View, Text, StyleSheet } from 'react-native'
import {
  neutral100,
  neutral300,
  neutral400,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  fontSizeBase,
  fontWeightBold,
  fontWeightMedium,
  spacing1,
  spacing2,
  spacing3,
  spacing4,
  radiusMd,
  shadowCard,
} from '@borne-map/ui/src/tokens/native'

interface ReviewCardProps {
  authorName: string
  rating: number
  text: string
  date: string
}

function StarRating({ rating }: { rating: number }) {
  const stars = []
  for (let i = 1; i <= 5; i++) {
    stars.push(
      <Text key={i} style={styles.star}>{i <= rating ? '★' : '☆'}</Text>,
    )
  }
  return <View style={styles.starsRow}>{stars}</View>
}

export default function ReviewCard({ authorName, rating, text, date }: ReviewCardProps) {
  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <View style={styles.authorRow}>
          <Text style={styles.authorName}>{authorName}</Text>
          <StarRating rating={rating} />
        </View>
        <Text style={styles.date}>{date}</Text>
      </View>
      <Text style={styles.text}>{text}</Text>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    padding: spacing4,
    marginHorizontal: spacing4,
    marginVertical: spacing1,
    ...shadowCard,
  },
  header: {
    marginBottom: spacing2,
  },
  authorRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  authorName: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightBold,
    color: neutral700,
  },
  starsRow: {
    flexDirection: 'row',
  },
  star: {
    fontSize: fontSizeBase,
    color: '#F59E0B',
    marginLeft: spacing1,
  },
  date: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
    marginTop: spacing1,
  },
  text: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
    lineHeight: 20,
  },
})
