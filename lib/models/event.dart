import 'app_region.dart';

class Event {
  final String id;
  final String title;
  final String date;
  final String location;
  final String description;
  final String source;
  final String url;
  final double latitude;
  final double longitude;

  const Event({
    required this.id,
    required this.title,
    required this.date,
    required this.location,
    required this.description,
    required this.source,
    required this.url,
    required this.latitude,
    required this.longitude,
  });

  factory Event.fromJson(Map<String, dynamic> json) {
    return Event(
      id: json['id'] as String? ?? '',
      title: json['title'] as String? ?? '',
      date: json['date'] as String? ?? '',
      location: json['location'] as String? ?? '',
      description: json['description'] as String? ?? '',
      source: json['source'] as String? ?? '',
      url: json['url'] as String? ?? '',
      latitude: (json['latitude'] as num?)?.toDouble() ?? 36.2168,
      longitude: (json['longitude'] as num?)?.toDouble() ?? -81.6746,
    );
  }

  DateTime? get dateObject => date.isNotEmpty ? DateTime.tryParse(date) : null;

  AppRegion? get region => AppRegionX.infer(location);
}
